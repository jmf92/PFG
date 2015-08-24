package pfg.Siddhi;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.stream.input.InputHandler;
import pfg.Kafka.KafkaConsumerManager;
import pfg.Kafka.KafkaProducer;
import pfg.Rest.RestListener;


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


/**
 * Created by jmf on 8/07/15.
 */
public class SiddhiHandler implements RestListener{
    private static final Logger log = LoggerFactory.getLogger(SiddhiHandler.class);

    private ObjectMapper mapper;
    private String queries;
    private String InputStream;
    private KafkaProducer producer;
    private KafkaConsumerManager consumer;
    private Siddhi siddhi;
    private InputHandler inputHandler;
    private int threads = 1;
    private Map<String, Map<String, Object>> rawQueries = new HashMap<String, Map<String, Object>>();
    private boolean isRestart = false;
    private boolean run = false;

    private ArrayList outStream;
    private boolean update;

    public SiddhiHandler(KafkaConsumerManager kafkaConsumer, KafkaProducer kafkaProducer){
        this.producer = kafkaProducer;
        this.consumer = kafkaConsumer;
        this.mapper = new ObjectMapper();
        this.outStream = new ArrayList();
        this.queries = "";

    }

    public InputHandler getInputHandler() {
        return inputHandler;
    }
    public boolean isRestart() {
        return isRestart;
    }

    public void startSiddhi(){
        //Preparamos e iniciamos el motor de correlación
        siddhi = new Siddhi(InputStream, queries, producer);
        siddhi.start(outStream);
        inputHandler = siddhi.getInputHandler();

        // Arrancamos, si no lo estaba ya, el consumidor de Kafka que
        // será el encargado de generar las entradas del motor
        if (!run)
            run();
    }

    public void run(){
        this.run = true;
        try {
            //Obtenemos los logs del topic de kafka, que serán la entrada del motor
            consumer.run(threads, this);
            Thread.sleep(10000);
        } catch (InterruptedException ie) {
            log.error(null, ie);
        }

    }


    public void stopSiddhi(){
        if(siddhi != null)
            siddhi.stop();

    }

    public void stop(){
        if(consumer != null)
            consumer.shutdown();
        else
            log.error("Error while shutting down kafka KafkaConsumer");

        if (producer != null)
            producer.closeProducer();
        else
            log.error("Error while shutting down Kafka Producer");


    }




    @Override
    public boolean restartExecutionPlan(String newInputStream) {
        Map<String, Object> inputStream;
        boolean result = false;
        try {
            // Comprobamos que el string que define el stream de entrada
            // no esté vacio.
            // Si lo está es que no se quiere cambiar la definición del stream anterior
            // Si no lo actualizamos.
            if(!newInputStream.isEmpty()) {
                inputStream = mapper.readValue(newInputStream, Map.class);
                InputStream="";
                InputStream = inputStream.get("stream").toString();
                result = true;

                log.info("String stream:{}", newInputStream);
                log.info("Input:{}", InputStream);
                log.info("Queries:{}", queries);
            }

            //Si no está activo el flag de reinicio lo activamos
            if(!isRestart)
                isRestart = true;

            log.info("Restart is upload");




         } catch (IOException e) {
        log.debug("Exception! {}", e.getMessage());
        log.error("Couldn't parse JSON query {}", newInputStream);
    }


        return result;
    }


    public void restartSiddhi(){
        //Si hay peticiones de reinicio:
        //-Se comprueba si hay que actualizar las queries
        //-Paramos el plan ejecución anterior e iniciamos
        // el actualizado

            if(isRestart){
                //Bajamos el flag de reinicios pendientes
                isRestart = false;
                log.info("Restart request is received");


                //Paramos el motor para su posterior reinicio
                this.stopSiddhi();
                log.info("Upgrade is starting...");

                // Comprobamos si hay que actualizar los componentes del motor, asociados a las queries:
                // +Queries
                // +Stream salida
                if (update)
                    updateQueries();

                log.info("Consumer:{}",consumer.toString());
                log.info("Producer:{}", producer.toString());

                //Iniciamos el motor
                this.startSiddhi();
                log.info("Consumer:{}",consumer.toString());
                log.info("Producer:{}", producer.toString());
                log.info("Siddhi:{}", siddhi.toString());
            }


    }


    public void updateQueries(){
        log.info("Updating queries. . . ");

        // Si hay peticiones pendientes de inclusión/eliminación
        // actualizamos el valor del string  que contiene las queries
        // y el array que contiene los streams de salidas asociados a dichas queries.
        if (update){
            queries ="";
            if(!outStream.isEmpty())
                outStream.clear();
            for( Object query : rawQueries.keySet().toArray()){
                queries = queries + rawQueries.get(query).get("query").toString();
                outStream.add(rawQueries.get(query).get("OutStream"));
            }
            update = false;
        }


    }




    public boolean add(String newQuery) {
        Map<String, Object> query;
        boolean result = false;

        try {
            query = mapper.readValue(newQuery, Map.class);

            String id = query.get("id").toString();

            // Comprobamos que el identificador de query, y por tanto la query,
            // no esté repetido
            if (rawQueries.containsKey(id)) {
                log.error("Query with id {} already exist", id);
            } else {

                // Si el stream de salida es válido, no está registrado aún,
                // actualizamos las queries
                if(!outStream.contains(query.get("OutStream"))){
                    result = true;

                    //Añadimos la query
                    rawQueries.put(id, query);

                    // Subimos el flag de actualizaciones pendientes.
                    if(!update)
                        update = true;

                    log.info("Current queries: {}", rawQueries.toString());
                    log.info("Siddhi queries: {}", queries);
                    log.info("New query added: {}", query);
                }
                else
                    log.error("Query with outputStream{} is already used", query.get("OutStream"));
            }
        } catch (IOException e) {
            log.debug("Exception! {}", e.getMessage());
            log.error("Couldn't parse JSON query {}", newQuery);
        }

        return result;
    }


    public boolean remove(String id) {
        boolean removed = (rawQueries.remove(id) != null);


        // Si ha sido eliminada con éxito, actualizamos las queries
        // y el stream de salida asociado a ésta
        if (removed) {

            // Subimos el flag de actualizaciones pendientes.
            if(!update)
                update = true;

            log.info("Query with the id {} has been removed", id);
            log.info("Current queries: {}", rawQueries);
        } else {
            log.error("Query with the id {} is not present", id);
        }

        return removed;
    }


}

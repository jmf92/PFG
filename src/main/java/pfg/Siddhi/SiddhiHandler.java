package pfg.Siddhi;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.stream.input.InputHandler;
import pfg.Kafka.KafkaConsumerManager;
import pfg.Kafka.KafkaProducer;
import pfg.KafkaSiddhiConnector;
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
    private KafkaSiddhiConnector connector;
    private int threads = 1;
    private Map<String, Map<String, Object>> rawQueries = new HashMap<String, Map<String, Object>>();
    private boolean isRestart = false;
    private boolean addResult = false;
    private boolean run = false;

    private ArrayList outStream;

    public SiddhiHandler(KafkaConsumerManager kafkaConsumer, KafkaProducer kafkaProducer){
        this.producer = kafkaProducer;
        this.consumer = kafkaConsumer;
        this.mapper = new ObjectMapper();
        this.outStream = new ArrayList();
        this.queries = "";
        this.connector = KafkaSiddhiConnector.getInstance();

        //Arrancamos el consumidor de Kafka que genera la entrada del motor
        //startSiddhi();
        //run();


    }

    public void run(){
        this.run = true;
        try {
            //Obtenemos los logs del topic de kafka, que serán la entrada del motor
            //log.info("Se inicia el consumo de la entrada con id{}", inputHandler.getStreamId() );
            consumer.run(threads);
            Thread.sleep(10000);
        } catch (InterruptedException ie) {
            log.error(null, ie);
        }

    }

    public void startSiddhi(){
        siddhi = new Siddhi(InputStream, queries, producer);
        siddhi.start(outStream);
        inputHandler = siddhi.getInputHandler();
        connector.setInputHandler(inputHandler);

        if (!run)
            run();
    }

    public boolean isRestart() {
        return isRestart;
    }

    public void setIsRestart(boolean isRestart) {
        this.isRestart = isRestart;
    }

    public void start(String InputStream, String queries){

        //Preparamos e iniciamos el motor de correlación
        startSiddhi();

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
            log.debug("No hay reinicio");
            if(isRestart){
                //Bajamos el flag de reinicios pendientes
                isRestart = false;
                log.info("Restart request is received");



                //Comprobamos si hay queries pendientes de insertar
                // en el plan de ejecución
                if(addResult){
                    log.info("Queries to add");
                    updateQueries();
                }


                //Paramos el motor para su posterior reinicio
                this.stopSiddhi();
                log.info("Upgrade is starting...");

                log.info("Consumer:{}",consumer.toString());
                log.info("Producer:{}", producer.toString());
                log.info("Connector:{}", connector.toString());
                this.start(InputStream, queries);
                log.info("Consumer:{}",consumer.toString());
                log.info("Producer:{}", producer.toString());
                log.info("Connector:{}", connector.toString());
                log.info("Siddhi:{}", siddhi.toString());
            }


    }


    public void updateQueries(){
        log.info("Update request has been started");

       if(addResult)
        {
            //queries ="";
            //Actualizamos el string que contiene las queries
            for (Map.Entry entry :rawQueries.entrySet().iterator().next().getValue().entrySet()){


                //queries.concat(rawQueries.get("query").toString());
                log.info("Queries-key:{}", entry.getKey().toString());

                log.info("Queries-value:{}", entry.getValue().toString());

            }
            addResult = false;
        }
    }




    public boolean add(String newQuery) {
        Map<String, Object> query;
        boolean result = false;

        try {
            query = mapper.readValue(newQuery, Map.class);

            String id = query.get("id").toString();

            if (rawQueries.containsKey(id)) {
                log.error("Query with id {} already exist", id);
            } else {

                if(!outStream.contains(query.get("OutStream"))){
                    result = true;

                    queries= queries+ query.get("query").toString();
                    //Añadimos la query
                    rawQueries.put(id, query);

                    //Añadimos el stream de salida asociado a la query para el posterior
                    //lanzamiento de los callbacks asociados a dichos streams
                    outStream.add(outStream.size(),query.get("OutStream"));


                    //Activamos el flag de queries pendientes de insertar
                    if(!addResult)
                        addResult = true;

                    log.info(rawQueries.toString());
                    log.info(queries);
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

        if (removed) {
            log.info("Query with the id {} has been removed", id);
            log.info("Current queries: {}", rawQueries);
        } else {
            log.error("Query with the id {} is not present", id);
        }

        return removed;
    }


}

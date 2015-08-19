package pfg.Kafka;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

import org.codehaus.jackson.map.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.stream.input.InputHandler;
import pfg.KafkaSiddhiConnector;
import pfg.Siddhi.SiddhiHandler;

import java.util.Map;
import java.util.logging.Level;

/**
 * Esta clase se encarga de consumir los mensajes guardados en Apache Kafka
 * y generar los objetos de entrada al motor de correlación(Siddhi).
 *
 * @author Jaime Márquez Fernández
 *
 */



public class KafkaConsumer implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);

    private KafkaStream m_stream;
    private int m_threadNumber;
    private InputHandler inputHandler;
    private Object[] JsonSchema;
    private Object[] JsonAttributes;
    private KafkaConsumerManager consumerGE;

    private KafkaSiddhiConnector connector;


    public Object[] getJsonAttributes() {
        return JsonAttributes;
    }

    public void setJsonAttributes(Object[] jsonAttributes) {
        JsonAttributes = jsonAttributes;
    }

    public Object[] getJsonSchema() {
        return this.JsonSchema;
    }

    public void setJsonSchema(Object[] jsonSchema) {
        this.JsonSchema = jsonSchema;
    }




    public KafkaConsumer(KafkaStream a_stream, int a_threadNumber, KafkaConsumerManager consumerGroupExample) {
        this.m_threadNumber = a_threadNumber;
        this.m_stream = a_stream;
        this.consumerGE = consumerGroupExample;
        this.connector = KafkaSiddhiConnector.getInstance();

    }

    public void run() {
        ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
        String linea;

        log.info("El consumidor llega a arrancar{}",connector.toString());

        //Mientras el motor no esté preparado no iniciamos el consumo de mensajes
        while (connector.getInputHandler() == null){
            log.info("Esperando que siddhi inicie");
            try {
                wait(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        log.info("Comienza el consumo de KAfka");
        //Mientras el iterador indique que hay más elementos que leer
        //Seguiremos generando entradas en el motor
        while (it.hasNext()) {

            log.info("Thread " + m_threadNumber + ": " + new String(it.next().message()));

            //Obtenemos la linea leida por el consumidor
            linea = new String(it.next().message());
            ObjectMapper mapper = new ObjectMapper();


            try {
                //Obtenemos el JSON de la linea leida
                Map<String, Object> json = mapper.readValue(linea, Map.class);
                JsonSchema = new Object[json.size()];
                JsonAttributes = new Object[json.size()];
                int i = 0;

                //Extraemos esquema y valor de atributos del JSON obtenido anteriormente
                for (Map.Entry entry : json.entrySet()) {
                    JsonSchema[i] = entry.getKey();
                    JsonAttributes[i] = entry.getValue();

                    log.debug(JsonSchema[i].toString() + ":" + JsonAttributes[i].toString());
                    i++;
                }



                //Comprobamos que el manejador esté listo
                if(connector.getInputHandler() != null) {

                    //Comprobamos que el manejador siga siendo el mismo
                    if (inputHandler != connector.getInputHandler()){

                        //Obtenemos el manejador de entrada desde el conector a siddhi
                        inputHandler = connector.getInputHandler();
                    }

                    //Enviamos el valor de los atributos extraidos del objeto JSON al input de Siddhi
                    inputHandler.send(JsonAttributes);
                }
                else
                    log.error("Siddhi input is out of service");
                //Conservamos el esquema JSON para su posterior recuperación
                consumerGE.setJsonSchema(JsonSchema);


            } catch (Exception ie) {
                log.error(null, ie);
            }
        }


        log.info("Shutting down Thread: " + m_threadNumber);
    }
}
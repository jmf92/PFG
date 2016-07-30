package pfg.Kafka;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

import org.codehaus.jackson.map.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.stream.input.InputHandler;
import pfg.Siddhi.SiddhiHandler;

import java.util.Map;

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
    private SiddhiHandler siddhiHandler;


    public KafkaConsumer(KafkaStream a_stream, int a_threadNumber, SiddhiHandler siddhiHandler) {
        this.m_threadNumber = a_threadNumber;
        this.m_stream = a_stream;
        this.siddhiHandler = siddhiHandler;
    }

    public void run() {
        ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
        String linea;

        log.info("Kafka consumer is starting...");
        //Mientras el iterador indique que hay más elementos que leer
        //Seguiremos generando entradas en el motor
        while (it.hasNext()) {


            //Obtenemos la linea leida por el consumidor
            linea = new String(it.next().message());
            log.info("Thread: {}\t Message consumed:{}", m_threadNumber,linea);

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



                //Comprobamos que el manejador de entrada del motor esté listo
                if(siddhiHandler.getInputHandler() != null) {

                    //Comprobamos si el manejador sigue siendo el mismo
                    if (inputHandler != siddhiHandler.getInputHandler()){

                        //Obtenemos el manejador de entrada desde el conector a siddhi
                        inputHandler = siddhiHandler.getInputHandler();
                    }

                    //Enviamos el valor de los atributos extraidos del objeto JSON al input de Siddhi
                    inputHandler.send(JsonAttributes);
                }
                else
                    log.error("Siddhi input is out of service");

            } catch (Exception ie) {
                log.error(null, ie);
            }
        }


        log.info("Shutting down Thread: " + m_threadNumber);
    }
}
package pfg.Kafka;

/**
 * Esta clase tiene como función transformar la salida del motor de correlación
 * a mensajes Kafka
 *
 * @author Jaime Márquez Fernández
 */


import java.io.IOException;
import java.util.*;
import java.util.logging.Level;


import kafka.common.KafkaException;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.event.Event;

public class KafkaProducer {
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);
    private Producer producer;
    private KeyedMessage<String, String> data;
    private Object[] JsonSchema;
    private ObjectMapper mapper;


    public KafkaProducer() {

        //Propiedades para poder conectar el productor a Kafka
        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        //props.put("partitioner", "example.producer.SimplePartitioner");
        props.put("request.required.acks", "1");

        ProducerConfig config = new ProducerConfig(props);

        producer = new Producer<String, String>(config);
        mapper = new ObjectMapper();




    }

    public void prepareProducer(Object[] jsonSchema){
        this.JsonSchema = jsonSchema;
    }
    public void useProducer(Event event) throws IOException {

        Map<String, Object> HashJson = new HashMap<String, Object>();

        //Generamos el JSON de salida con los datos ofrecidos por el motor de correlación
        for (int i = 0; i< event.getData().length; i++)
        {
            HashJson.put(JsonSchema[i].toString(), event.getData(i).toString());
        }

        log.info(mapper.writeValueAsString(HashJson));

        //Creamos el mensaje Kafka que será enviado
        data = new KeyedMessage<String, String>("rb_vault_post", mapper.writeValueAsString(HashJson));


        try {
            //Enviamos el mensaje producido
            producer.send(data);


        }catch (KafkaException ke){
            log.error( "Send kafka message is not completed", ke);

        }

    }
    public void closeProducer() {
        log.info("Shutting down KafkaProducer");
        producer.close();
    }
}


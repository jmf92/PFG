package pfg.Kafka;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.wso2.siddhi.core.stream.input.InputHandler;
import pfg.Siddhi.SiddhiHandler;
import pfg.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Esta clase se encarga de manejar los consumidores y
 * establecer la configuracion Kafka y Zookeeper de éstos,
 * para que se puedan conectarse a Kafka.
 *
 * @author Jaime Márquez Fernández
 */
public class KafkaConsumerManager {
    private ConsumerConnector consumer;
    private  ExecutorService executor;


    private String zookeeper;
    private String groupId;
    private String topic;
    private String kafkaBroker;

    private Object[] JsonSchema;

    public KafkaConsumerManager(String a_zookeeper, String a_groupId, String a_topic, String a_kafkaBroker) {
        this.zookeeper = a_zookeeper;
        this.groupId = a_groupId;
        this.topic = a_topic;
        this.kafkaBroker = a_kafkaBroker;


        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                createConsumerConfig(a_kafkaBroker, a_zookeeper, a_groupId ));

        /*this.topicCountMap = new HashMap<String, Integer>();
        this.topicCountMap.put(topic, new Integer(1));
        this.consumerMap = consumer.createMessageStreams(topicCountMap);
        this.streams = consumerMap.get(topic);*/
    }

    public void shutdown() {
        if (consumer != null) consumer.shutdown();
        if (executor != null) executor.shutdown();
        try {
            if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {

                Logger.getLogger(KafkaProducer.class.getName()).log(Level.INFO,
                        "Timed out waiting for consumer threads to shut down, exiting uncleanly");
            }
        } catch (InterruptedException e) {
            Logger.getLogger(Test.class.getName()).log(Level.SEVERE,
                    "Interrupted during shutdown, exiting uncleanly",e);
        }
    }
    /*
    public void restartConsumer(int a_numThreads, InputHandler inputHandler){

        shutdown();
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                createConsumerConfig(kafkaBroker, zookeeper, groupId ));
        run(a_numThreads, inputHandler);
    }*/

    public void run(int a_numThreads) {
        Map<String, Integer> topicCountMap =new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(a_numThreads));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);


        // Creamos el manejador del hilo en el que se lanzara el consumidor
        executor = Executors.newFixedThreadPool(a_numThreads);

        //Creamos un objeto encargado de consumir los mensajes
        int threadNumber = 0;
        for (final KafkaStream stream : streams) {
            KafkaConsumer consumer = new KafkaConsumer(stream, threadNumber, this);
            executor.submit(consumer);
            setJsonSchema(consumer.getJsonSchema());
            threadNumber++;
        }
    }

    public void setJsonSchema(Object[] jsonSchema){

        this.JsonSchema = jsonSchema;


    }

    public Object[] getJsonSchema(){
        return this.JsonSchema;
    }

    private static ConsumerConfig createConsumerConfig(String a_kafkaBroker, String a_zookeeper, String a_groupId) {

        Properties props = new Properties();

        //Propiedades kafka
        props.put("metadata.broker.list", a_kafkaBroker);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");

        //Propiedades zookeeper
        props.put("zookeeper.connect", a_zookeeper);
        props.put("group.id", a_groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "smallest");

        return new ConsumerConfig(props);
    }

}



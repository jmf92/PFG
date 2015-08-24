package pfg;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pfg.Kafka.KafkaConsumerManager;
import pfg.Kafka.KafkaProducer;
import pfg.Siddhi.SiddhiHandler;
import pfg.Rest.RestManager;
import pfg.Siddhi.StartChecking;
import pfg.Util.ConfigData;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;


/**
 * Created by jmf on 13/05/15.
 */
public class CorrelationService {
    public static final String DEFAULT_CONFIG_FILE = "./config/configFileCS.yml";
    private static final Logger log = LoggerFactory.getLogger(CorrelationService.class);

    public static void main(String[] args) throws InterruptedException, IOException {
        ConfigData configData = new ConfigData();
        Map configMap;
        //Obtenemos la configuración, mediante fichero, de: kafka, zookeeper y REST
        //Si no se pasa ningún fichero, fijamos la configuración por defecto
        if (args.length >= 1)
            configMap=configData.load(args[0]);
        else
            configMap=configData.load(DEFAULT_CONFIG_FILE);

        //Si la configuración ha sido cargada exitosamente iniciamos el servicio
        if(configMap != null) {
            //Informamos de la configuración que será establecida
            log.info("Zookeeper: {} ", configMap.get("zookeeperURL").toString());
            log.info("ZookeeperGroup: {}", configMap.get("zookeeperGroup").toString());
            log.info("zookeeperSessionTimeout: {} ", configMap.get("zookeeperSessionTimeout").toString());
            log.info("zookeeperSyncTime: {} ", configMap.get("zookeeperSyncTime").toString());
            log.info("zookeeperCommitInterval: {} ", configMap.get("zookeeperCommitInterval").toString());
            log.info("zookeeperOffsetReset: {} ", configMap.get("zookeeperOffsetReset").toString());


            log.info("Kafkabroker: {}", configMap.get("kafkaBrokers").toString());
            log.info("kafkaSerializer: {}", configMap.get("kafkaSerializer").toString());
            log.info("kafkaACK: {}", configMap.get("kafkaACK").toString());
            log.info("consumerTopic: {}", configMap.get("consumerTopic").toString());
            log.info("producerTopic: {}", configMap.get("producerTopic").toString());

            log.info("RestURI: {}", configMap.get("RestURI").toString());


            //Creamos un consumidor con las propiedades anteriormente inicializadas
            final KafkaConsumerManager kafkaConsumer = new KafkaConsumerManager(
                    configMap.get("kafkaBrokers").toString(),
                    configMap.get("kafkaSerializer").toString(),
                    configMap.get("kafkaACK").toString(),
                    configMap.get("consumerTopic").toString(),
                    configMap.get("zookeeperURL").toString(),
                    configMap.get("zookeeperGroup").toString(),
                    configMap.get("zookeeperSessionTimeout").toString(),
                    configMap.get("zookeeperSyncTime").toString(),
                    configMap.get("zookeeperCommitInterval").toString(),
                    configMap.get("zookeeperOffsetReset").toString()
                    );

            //Creamos un productor
            final KafkaProducer kafkaProducer = new KafkaProducer(configMap.get("kafkaBrokers").toString(),
                    configMap.get("kafkaSerializer").toString(),
                    configMap.get("kafkaACK").toString(),
                    configMap.get("producerTopic").toString());

            //Creamos el manejador de siddhi
            SiddhiHandler siddhiHandler = new SiddhiHandler(kafkaConsumer, kafkaProducer);

            StartChecking startChecking = new StartChecking(siddhiHandler);

            // RestManager inicia la API REST y redirige las peticiones de inclusion/eliminación
            // que los usuarios añadirán a Siddhi.
            RestManager.startServer(siddhiHandler, configMap.get("RestURI").toString());


            //Iniciamos el hilo que comprobará las peticiones de reinicio
            startChecking.run();

            //Iniciamos el motor de correlación
            //siddhiHandler.start(executionPlan, queries);
            //siddhiHandler.recharge();
            //Finalizamos el motor  tras un tiempo de guarda
            //siddhiHandler.stop();
        }
        else
            log.error("Configuration is not loaded correctly");


    }


}

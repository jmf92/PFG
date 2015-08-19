package pfg;

import pfg.Kafka.KafkaConsumerManager;
import pfg.Kafka.KafkaProducer;
import pfg.Siddhi.SiddhiHandler;
import pfg.Rest.RestManager;
import pfg.Siddhi.StartChecking;

import java.io.FileNotFoundException;
import java.io.IOException;


/**
 * Created by jmf on 13/05/15.
 */
public class Test {
    public static void main(String[] args) throws InterruptedException, FileNotFoundException, IOException {


        //Configuración para el consumidor de kafka
        String zooKeeper = "localhost:2181";
        String groupId = "logs_38";
        String topic = "rb_vault";
        final String kafkaBroker = "localhost:9092";

        //Configuración para el motor de correlación Siddhi
        String executionPlan = "@config(async = 'true')define stream sshStream (" +
                "message string,"+
                "raw_message string,"+
                "host string,"+
                "fromhost string,"+
                "fromhost_ip string,"+
                "syslogtag string,"+
                "program_name string," +
                "pri string,"+
                "pri_text string,"+
                "iut string,"+
                "syslogfacility string,"+
                "syslogfacility_text string,"+
                "syslogseverity string,"+
                "syslogseverity_text string,"+
                "syslogpriority string,"+
                "syslogpriority_text string,"+
                "timegenerated string,"+
                "protocol_version string,"+
                "structured_data string,"+
                "app_name string,"+
                "procid string,"+
                "msgid string,"+
                "inputname string,"+

                "method string," +
                "auth_method string," +
                "user string,"+
                "ip string,"+
                "port string,"+
                "protocol string" +
                ");";


        String queries = "@info(name = 'query1') from sshStream[user == 'root'] select ip, port insert into OutsshStream;";


        //Creamos un consumidor con las propiedades anteriormente inicializadas
        final KafkaConsumerManager kafkaConsumer = new KafkaConsumerManager(zooKeeper, groupId, topic, kafkaBroker);

        //Creamos un productor
        final KafkaProducer kafkaProducer = new KafkaProducer();

        //Creamos el manejador de siddhi
        SiddhiHandler siddhiHandler = new SiddhiHandler(kafkaConsumer, kafkaProducer);

        StartChecking startChecking = new StartChecking(siddhiHandler);

        // RestManager inicia la API REST y redirige las peticiones de inclusion/eliminación
        // que los usuarios añadirán a Siddhi.
        RestManager.startServer(siddhiHandler);



/*
        try {
            //Obtenemos los logs del topic de kafka, que serán la entrada del motor
            //log.info("Se inicia el consumo de la entrada con id{}", inputHandler.getStreamId() );

            kafkaConsumer.run(1);
            Thread.sleep(10000);
            System.out.println("Consumo acabado");

        } catch (InterruptedException ie) {
            //log.error(null, ie);
        }*/

        //Iniciamos el hilo que comprobará las peticiones de reinicio
        startChecking.run();

        //Iniciamos el motor de correlación
        //siddhiHandler.start(executionPlan, queries);
        //siddhiHandler.recharge();
        //Finalizamos el motor  tras un tiempo de guarda
        //siddhiHandler.stop();

    }


}

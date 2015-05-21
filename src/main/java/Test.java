
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

import java.io.FileNotFoundException;
import java.io.IOException;

import java.util.Arrays;




/**
 * Created by jmf on 13/05/15.
 */
public class Test {
    public static void main(String[] args) throws InterruptedException, FileNotFoundException, IOException {

        // Create Siddhi Manager
        SiddhiManager siddhiManager = new SiddhiManager();
        String zooKeeper = "localhost:2181";
        String groupId = "logs";
        String topic = "rb_vault";
        String kafkaBroker = "localhost:9092";
        int threads = 1;

        //Creamos un consumidor con las propiedades anteriormente inicializadas
        ConsumerGroupExample example = new ConsumerGroupExample(zooKeeper, groupId, topic, kafkaBroker);


        String executionPlan = "@config(async = 'true')define stream sshStream (program_name string, method string, user string);"
                + "@info(name = 'query1') from sshStream#window.length(2) select program_name, method, user insert into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        executionPlanRuntime.start();

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {

                for(Event e : inEvents)
                    System.out.println(Arrays.asList(e.getData()));
                EventPrinter.print(timeStamp, inEvents, removeEvents);

            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("sshStream");



        try {
            //Obtengo los logs del topic de kafka
            example.run(threads, inputHandler);
            //enviaContenido("/var/log/norm_ssh.log", inputHandler);
            Thread.sleep(10000);
        } catch (InterruptedException ie) {

        }
        example.shutdown();



        /*
        inputHandler.send(new Object[]{"IBM", 700f, 100l});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 200l});
        inputHandler.send(new Object[]{"GOOG", 50f, 30l});
        inputHandler.send(new Object[]{"IBM", 76.6f, 400l});
        inputHandler.send(new Object[]{"WSO2", 45.6f, 50l});
        */

    }


}

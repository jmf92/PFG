package pfg.Siddhi;

import kafka.Kafka;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.query.api.ExecutionPlan;
import pfg.Kafka.KafkaProducer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by jmf on 18/08/15.
 */
public class Siddhi {
    private static final Logger log = LoggerFactory.getLogger(Siddhi.class);

    private SiddhiManager siddhiManager;
    private ExecutionPlanRuntime executionPlanRuntime;
    private InputHandler inputHandler;
    private KafkaProducer kafkaProducer;


    public Siddhi(String inputStream, String queries, KafkaProducer producer){

        log.info("InputStream:{}\n Queries:{}\n", inputStream, queries);

        this.kafkaProducer = producer;
        this.siddhiManager = new SiddhiManager();
        this.executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inputStream+queries);

    }
    public InputHandler getInputHandler(){
        return this.inputHandler;
    }
    public void start(String nameInputStream, ArrayList outStream){

        //Añadimos los callbacks
        //Creamos los callbacks para la salida generada por el motor
        addCallbacks(outStream);


        //Definimos el manejador de los eventos de entrada del motor
        inputHandler = executionPlanRuntime.getInputHandler(nameInputStream);
        log.info("Generating input handler");

        //Ejecutamos el plan de ejecución
        executionPlanRuntime.start();
        log.info("Starting execution Plan Runtime");

    }
    public void stop(){
        if (executionPlanRuntime != null)
            executionPlanRuntime.shutdown();

        if(siddhiManager != null)
            siddhiManager.shutdown();

    }

    public void addCallbacks(ArrayList OutStream){

        for (Object outStream: OutStream){
            log.info("Starting callback:{}", outStream.toString());
            addCallback(outStream.toString());
        }

    }

    public void addCallback(final String Stream){

        executionPlanRuntime.addCallback(Stream, new StreamCallback() {
            @Override
            public void receive(Event[] inEvents) {
                log.info("Callback{} is working", Stream);
                log.info("Producer{}", kafkaProducer.toString());
                log.info("ExecPlanRunTime{}", executionPlanRuntime);

                //Preparamos el productor con el esquema del Stream de salida
                kafkaProducer.prepareProducer(executionPlanRuntime.getStreamDefinitionMap().get(Stream).getAttributeNameArray());

                for (Event e : inEvents) {

                    log.info("Consumed event:{} ", e);

                    //Iniciamos el productor
                    try {
                        kafkaProducer.useProducer(e);
                    } catch (IOException ioe) {
                        log.error(null, ioe);
                    }


                }
                //kafkaProducer.closeProducer();
            }
        });
        log.info("Callback with id {} is created", Stream);
        log.info("Producer{}", kafkaProducer.toString());
        log.info("ExecPlanRunTime{}", executionPlanRuntime);
    }

}

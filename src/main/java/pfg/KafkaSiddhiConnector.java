package pfg;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.stream.input.InputHandler;

/**
 * Created by jmf on 18/08/15.
 */
public class KafkaSiddhiConnector {
    private static final Logger log = LoggerFactory.getLogger(KafkaSiddhiConnector.class);

    private static KafkaSiddhiConnector instanceConnector;
    private InputHandler inputHandler = null;


    private KafkaSiddhiConnector(){
    }

    public static KafkaSiddhiConnector getInstance(){
        if (instanceConnector == null)
            instanceConnector = new KafkaSiddhiConnector();

        return instanceConnector;
    }


    public void setInputHandler(InputHandler inputHandler){
        this.inputHandler = inputHandler;
        log.info("InputHandler was changed to: {}", inputHandler);
    }
    public InputHandler getInputHandler(){
        return this.inputHandler;
    }
}

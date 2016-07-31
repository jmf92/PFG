package pfg.Siddhi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pfg.Rest.RestListener;

/**
 * Created by jmf on 19/08/15.
 */
public class StartChecking implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(StartChecking.class);
    private SiddhiHandler siddhiHandler;

    public StartChecking(SiddhiHandler siddhiHandler){
        log.info("Thread(StartChecking) is waiting start request...");
        this.siddhiHandler = siddhiHandler;
    }

    @Override
    public void run(){
        if(siddhiHandler != null) {
            while (true) {
                log.debug("Waiting requests...");
                if(siddhiHandler.isStart()){
                    log.info("Restart request was received");
                    siddhiHandler.start();
                }
            }
        }
        else
            log.error("Siddhi handler was not started");
    }

}

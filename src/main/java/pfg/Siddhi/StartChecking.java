package pfg.Siddhi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jmf on 19/08/15.
 */
public class StartChecking implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(StartChecking.class);
    private SiddhiHandler siddhiHandler;

    public StartChecking(SiddhiHandler siddhiHandler){
        log.info("hilo creado");
        this.siddhiHandler = siddhiHandler;
    }

    @Override
    public void run(){
        if(siddhiHandler != null) {
            while (true) {
                log.debug("Waiting request´s...");
                if(siddhiHandler.isRestart()){
                    log.info("Restart request was received");
                    siddhiHandler.restartSiddhi();
                }
            }
        }
        else
            log.error("Siddhi handler was not started");
    }
}

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

import org.json.JSONObject;

import org.wso2.siddhi.core.stream.input.InputHandler;





public class ConsumerTest implements Runnable {
    private KafkaStream m_stream;
    private int m_threadNumber;
    String linea;
    JSONObject ObjetoJSON;
    InputHandler inputHandler;

    public ConsumerTest(KafkaStream a_stream, int a_threadNumber, InputHandler inputHandler) {
        m_threadNumber = a_threadNumber;
        m_stream = a_stream;
        this.inputHandler = inputHandler;

    }

    public void run() {
        ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
        while (it.hasNext()) {
            System.out.println("Thread " + m_threadNumber + ": " + new String(it.next().message()));
            //Enviamos los flujos a siddhi
            linea = new String(it.next().message());
            ObjetoJSON = new JSONObject(linea);


            try {
                inputHandler.send(new Object[]{ObjetoJSON.getString("program_name"),
                        ObjetoJSON.getString("method"),
                        ObjetoJSON.getString("user")});

            }
            catch (InterruptedException ie){

            }
        }




        System.out.println("Shutting down Thread: " + m_threadNumber);
    }
}
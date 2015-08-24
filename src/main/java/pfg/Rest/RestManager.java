package pfg.Rest;

import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

public class RestManager {
    private static final Logger log = LoggerFactory.getLogger(RestManager.class);

    // URI donde el servidor HTTP Grizzly escuchará las peticiones
    private static final String BASE_URI = "http://localhost:8888/myapp/";
    private static RestListener listener = null;
    private static ResourceConfig rc;
    private static HttpServer server;

    private RestManager() {}

    /**
     * Iniciamos el servidor HTTP Grizzly que ofrece los programas Java definidos en esta aplicación
     *
     */
    public static void startServer(RestListener rl, String uri) {
        //Creamos una configuración de fuentes que busca en el paquete pfg.Rest
        //los proveedores de JAX-RS(servidor aplicaciones java)
        rc = new ResourceConfig().packages("pfg.Rest");

        // Establecemos el "listener" para comunicar siddhi con la API REST
        listener = rl;

        // Creamos e iniciamos una instancia del servidor que escuchará en la URI definida
        server = GrizzlyHttpServerFactory.createHttpServer(URI.create(uri), rc);
        log.info("Starting server...");
    }

    public static void stopServer() {
        if (server != null) {
            server.shutdown();
        }
    }

    public static RestListener getListener() {
        return listener;
    }
}

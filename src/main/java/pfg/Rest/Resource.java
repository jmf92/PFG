package pfg.Rest;

import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Singleton
@Path("res")
public class Resource {
    /**
     * Este método maneja peticiones HTTP POST para la inclusión de nuevas "queries"
     * en el motor de correlación.
     *
     * Utiliza una interfaz con el manejador del motor para comunicar dicha información.
     *
     * @param json Cadena en formato JSON.
     *
     * @return Respuesta HTTP con el código apropiado tras tratar la petición.
     *
     * @author Jaime Márquez Fernández
     */

    @POST
    @Path("/action=add")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response addQuery(String json) {
        RestListener listener = RestManager.getListener();

        // Check if the listener accepted the data
        if (listener == null) {
            return Response.status(500).build();
        } else if (listener.add(json)) {
            return Response.status(200).entity(json).build();
        } else {
            return Response.status(202).entity(json).build();
        }
    }

    /**
     * Este método maneja peticiones HTTP GET para la obtención de las queries definidas
     * en el motor
     *
     * Utiliza una interfaz con el manejador del motor para comunicar dicha información.
     *
     *
     * @return Respuesta HTTP con el código apropiado tras tratar la petición.
     *
     * @author Jaime Márquez Fernández
     */

    @GET
    @Path("/queries")
    @Produces(MediaType.TEXT_PLAIN)
    public Response getQueries() {
        RestListener listener = RestManager.getListener();

        // Check if the listener accepted the data
        if (listener == null) {
            return Response.status(500).build();
        }
        else{
            return Response.status(200).entity(listener.getQueries()+'\n').build();
        }
    }


    /**
     * Este metodo maneja las peticiones HTTP POST para
     * iniciar el motor de correlación.
     *
     * Utiliza la interfaz con el manejador del motor para pasarle la información.
     *
     * @param newExecutionPlan Cadena en formato JSON.
     *
     * @return Respuesta HTTP con el código apropiado tras tratar la petición.
     *
     * @author Jaime Márquez Fernández
     */

    @POST
    @Path("/action=start")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response start(String newExecutionPlan) {
        RestListener listener = RestManager.getListener();


        // Check if the listener accepted the data
        if (listener == null) {
            return Response.status(500).build();
        } else if (listener.startExecutionPlan(newExecutionPlan)) {
            return Response.status(200).entity(newExecutionPlan).build();
        } else {
            return Response.status(202).entity("Execution plan is not defined yet\n").build();
        }
    }








    /**
     * Este método maneja las peticiones HTTP DELETE utilizadas parar eliminar la query con ID= " id"
     * aplicada a un determinado plan de ejecución en el motor.
     *
     * @param id Se pasa como parámetro el identificador de la query que se desea eliminar
     *
     * @return Respuesta HTTP con el codigo apropiado tras tratar la petición.
     */

    @DELETE
    @Path("/action=delete/query/{id}")
    public Response removeQuery(@PathParam("id") String id) {
        RestListener listener = RestManager.getListener();

        // Check if the listener accepted the operation
        if (listener == null) {
            return Response.status(500).build();
        } else if (listener.remove(id)) {
            return Response.status(200).build();
        } else {
            return Response.status(404).entity("Query with the id " + id + " is not present").build();
        }
    }
}

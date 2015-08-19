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
    @Path("/add")
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
     * Este metodo maneja las peticiones HTTP POST para
     * actualizar el motor de correlación(formato JSON).
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
    @Path("/ExecPlan/update")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response restart(String newExecutionPlan) {
        RestListener listener = RestManager.getListener();


        // Check if the listener accepted the data
        if (listener == null) {
            return Response.status(500).build();
        } else if (listener.restartExecutionPlan(newExecutionPlan)) {
            return Response.status(200).entity(newExecutionPlan).build();
        } else {
            return Response.status(202).entity(newExecutionPlan).build();
        }
    }








    /**
     * This methods handles HTTP DELETE requests.
     * It sends an remove operation to the listener passing it an ID.
     * Este método maneja las peticiones HTTP DELETE utilizadas parar eliminar las queries
     * aplicadas a un determinado plan de ejecución en el motor.
     *
     * @param id Se pasa como parámetro el identificador de la query que se desea eliminar
     *
     * @return Respuesta HTTP con el codigo apropiado tras tratar la petición.
     */

    @DELETE
    @Path("/delete/{id}")
    @Consumes (MediaType.APPLICATION_JSON)
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

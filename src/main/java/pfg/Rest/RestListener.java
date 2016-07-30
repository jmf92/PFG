package pfg.Rest;


public interface RestListener {
    boolean add(String newQuery);
    boolean remove(String id);
    String getQueries();
    boolean startExecutionPlan(String newExecutionPlan);
}
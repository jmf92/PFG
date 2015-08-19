package pfg.Rest;

public interface RestListener {
    boolean add(String newQuery);
    boolean remove(String id);
    boolean restartExecutionPlan(String newExecutionPlan);
}
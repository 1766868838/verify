package cn.info.verify;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class SQLTransformer {

    private String destinationDb;
    private String sourceDb;
    private Definition destination;
    private Definition source;
    private String objType;
    private int verbosity;
    private String destTbl = "";
    private String srcTbl = "";

    private static final Map<String, Function<SQLTransformer, List<String>>> TRANS_METHOD = Map.of(
            "DATABASE", SQLTransformer::transformDatabase,
            "TABLE", SQLTransformer::transformTable,
            "VIEW", SQLTransformer::transformView,
            "TRIGGER", SQLTransformer::transformTrigger,
            "PROCEDURE", SQLTransformer::transformRoutine,
            "FUNCTION", SQLTransformer::transformRoutine,
            "EVENT", SQLTransformer::transformEvent
    );

    public SQLTransformer(String destinationDb, String sourceDb, Definition destination, Definition source,
                          String objType, int verbosity, Map<String, Object> options) {
    }

    public List<String> transformDefinition(){

        Function<SQLTransformer, List<String>> method = TRANS_METHOD.get(this.objType);
        if (method == null) {
            throw new Error("Unknown object type '" + this.objType + "' for transformation.");
        }
        return method.apply(this);

    }

    private List<String> transformDatabase() { return null; }
    private List<String> transformTable() { return null; }
    private List<String> transformView() { return null; }
    private List<String> transformTrigger() { return null; }
    private List<String> transformRoutine() { return null; }
    private List<String> transformEvent() { return null; }
}

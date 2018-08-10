package pe.mrodas.jdbc;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * <pre>{@code UPDATE <table> SET <fieldsToUpdate> WHERE <filterFields>}</pre>
 * @author Marco Rodas
 */
public class SqlUpdate implements SqlQuery.Save {

    private final static String QUERY = "UPDATE <table> SET <fieldsToUpdate> WHERE <filterFields>";
    private final Map<String, Object> fieldsMap = new HashMap<>();
    private final Map<String, Object> filterFieldsMap = new HashMap<>();
    private final String table;

    public SqlUpdate(String table) {
        this.table = table;
    }

    @Override
    public SqlUpdate addField(String name, Object value) throws Exception {
        if (value != null) {
            Adapter.checkNotNullOrEmpty(name, "SqlUpdate: El nombre de un campo no puede ser vacío o nulo");
            fieldsMap.put(name.replace(" ", ""), value);
        }
        return this;
    }

    public SqlUpdate addFilter(String name, Object value) throws Exception {
        if (value != null) {
            Adapter.checkNotNullOrEmpty(name, "SqlUpdate: El nombre de un campo no puede ser vacío o nulo");
            filterFieldsMap.put(name.replace(" ", ""), value);
        }
        return this;
    }

    private void addParameter(SqlQuery sqlQuery, Map<String, Object> map) {
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            sqlQuery.addParameter(key, value);
        }
    }

    @Override
    public boolean isInsert() {
        return false;
    }

    @Override
    public int execute() throws Exception {
        return execute(null);
    }

    public int execute(Connection connection) throws Exception {
        String fieldsToUpdate = fieldsMap.keySet().stream()
                .map(fieldName -> fieldName + " = :" + fieldName)
                .collect(Collectors.joining(", "));
        String filterFields = filterFieldsMap.keySet().stream()
                .map(fieldName -> fieldName + " = :" + fieldName)
                .collect(Collectors.joining(" AND "));
        String preparedQuery = QUERY.replace("<table>", table)
                .replace("<fieldsToUpdate>", fieldsToUpdate)
                .replace("<filterFields>", filterFields);
        SqlQuery sqlQuery = (connection == null ? new SqlQuery() : new SqlQuery(connection, false))
                .setSql(preparedQuery);
        addParameter(sqlQuery, fieldsMap);
        addParameter(sqlQuery, filterFieldsMap);
        return sqlQuery.execute();
    }

}

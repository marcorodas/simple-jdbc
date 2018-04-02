package pe.mrodas.jdbc;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 *
 * @author skynet
 */
public class SqlInsert implements SqlQuery.Save {

    public interface SetterId {

        void set(int id);
    }

    private final String query = "INSERT INTO <table>(<fieldNames>) VALUES(<fieldValueTags>)";
    private final Map<String, Object> fieldsMap = new HashMap<>();
    private final String table;
    private SqlQuery.AutoGenKey autoGenKey;
    private SetterId setterId;

    public SqlInsert(String table) throws Exception {
        this(table, SqlQuery.AutoGenKey.NO_RETURN);
    }

    public SqlInsert(String table, SetterId setterId) throws Exception {
        this(table, SqlQuery.AutoGenKey.RETURN);
        this.setterId = setterId;
    }

    public SqlInsert(String table, SqlQuery.AutoGenKey autoGenKey) throws Exception {
        Adapter.checkNotNullOrEmpty(table, "SqlInsert: El nombre de la tabla no puede ser vacío o nulo");
        this.table = table.replace(" ", "");
        this.autoGenKey = autoGenKey;
    }

    @Override
    public SqlInsert addField(String name, Object value) throws Exception {
        if (value != null) {
            Adapter.checkNotNullOrEmpty(name, "SqlInsert: El nombre de un campo no puede ser vacío o nulo");
            fieldsMap.put(name.replace(" ", ""), value);
        }
        return this;
    }

    @Override
    public boolean isInsert() {
        return true;
    }

    @Override
    public int execute() throws Exception {
        return execute(null);
    }

    public int execute(Connection connection) throws Exception {
        String fieldNames = fieldsMap.keySet().stream().collect(Collectors.joining(","));
        String fieldValueTags = fieldsMap.keySet().stream().map(fieldName -> ":" + fieldName)
                .collect(Collectors.joining(", "));
        String preparedQuery = query.replace("<table>", table)
                .replace("<fieldNames>", fieldNames)
                .replace("<fieldValueTags>", fieldValueTags);
        SqlQuery sqlQuery = connection == null ? new SqlQuery() : new SqlQuery(connection, false);
        sqlQuery.setSql(preparedQuery, autoGenKey);
        for (Map.Entry<String, Object> entry : fieldsMap.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            sqlQuery.addParameter(key, value);
        }
        int result = sqlQuery.execute();
        if (setterId != null) {
            setterId.set(result);
        }
        return result;
    }
}

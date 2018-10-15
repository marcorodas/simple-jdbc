package pe.mrodas.jdbc;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneOffset;
import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

/**
 * Uso: <font color="yellow"><code>{@code
 * SqlQuery<Type> query = new SqlQuery<>();}</code></font><br>
 * No usar: <font color="red"><code>{@code
 * SqlQuery<List<ElementType>> query = new SqlQuery<>();}</code></font><br>
 * <i>(Excepción en los métodos execute)</i><br>
 * Usar en cambio: <font color="yellow"><code>{@code
 * SqlQuery<ElementType> query = new SqlQuery<>();}</code></font><br>
 * T obj = {@link #execute(Executor)}<br>{@code List<T>} list =
 * {@link #execute(ExecutorList)}<br>
 *
 * @param <T> tipo devuelto por un método execute. Para {@link #execute()} se ignora.
 * @author Marco Rodas
 */
public class SqlQuery<T> extends DBLayer {

    /**
     * Represent an Insert or Update operation
     */
    public interface Save {

        Save addField(String name, Object value) throws Exception;

        int execute() throws Exception;

        boolean isInsert();

        default boolean isUpdate() {
            return !isInsert();
        }

    }

    /**
     * Lectura de un ResultSet: <i>(Implementación)</i>
     * <pre><code>(ResultSet rs) -> {
     *  T obj = new T(/.../);
     *  while (rs.next()) {
     *      /.../
     *  }
     *  return obj;
     * }</code></pre>
     *
     * @param <T> tipo devuelto por la implementación de esta clase
     * @author Marco Rodas
     */
    public interface Executor<T> {

        T execute(ResultSet rs) throws Exception;

    }

    /**
     * Lectura de un ResultSet: <i>(Implementación)</i>
     * <pre><code>
     * (ResultSet rs,{@code List<T> list}) -> {
     *  while (rs.next()) {
     *      list.add(obj);
     *  }
     * }</code></pre>
     *
     * @param <T> Tipo de elemento de la lista {@code List<T>} devuelta por esta
     *            clase
     * @author Marco Rodas
     */
    public interface ExecutorList<T> {

        void execute(ResultSet rs, List<T> list) throws Exception;
    }

    public interface MapperConfig<T> {

        void config(SqlMapper mapper, T result, ResultSet rs) throws Exception;
    }

    public enum AutoGenKey {
        RETURN, NO_RETURN
    }

    private String query;
    private boolean returnGeneratedKeys;
    private final HashMap<String, Object> parameters = new HashMap<>();
    private final List<String> parameterNames = new ArrayList<>();
    private Optional<String> nullParameter;
    private MapperConfig<T> config;
    private Class<T> clazz;

    public SqlQuery() {
    }

    public SqlQuery(Class<T> clazz) {
        this.clazz = clazz;
    }

    public SqlQuery(Class<T> clazz, Connection connection, boolean autoCloseConnection) {
        super(connection, autoCloseConnection);
        this.clazz = clazz;
    }

    public SqlQuery(Connection connection, boolean autoCloseConnection) {
        super(connection, autoCloseConnection);
    }

    /**
     * Establece el query. Uso:
     * <pre><code>
     * query.setSql("SELECT column1, column2 FROM table WHERE column2 = :column2");
     * </code></pre>
     *
     * @param sql
     * @return El mismo objeto SqlQuery
     */
    public SqlQuery<T> setSql(String sql) {
        return setSql(sql, AutoGenKey.NO_RETURN);
    }

    /**
     * Establece el query. Uso:
     * <pre><code>
     * query.setSql(new String[]{
     *  "SELECT column1, column2",
     *  "   FROM table",
     *  "   WHERE column2 = :column2"
     * });
     * </code></pre>
     *
     * @param sql
     * @return El mismo objeto SqlQuery
     */
    public SqlQuery<T> setSql(String[] sql) {
        return setSql(sql, AutoGenKey.NO_RETURN);
    }

    public SqlQuery<T> setSql(String sql, AutoGenKey generatedKeys) {
        returnGeneratedKeys = generatedKeys == AutoGenKey.RETURN;
        this.query = sql;
        return this;
    }

    public SqlQuery<T> setSql(String[] sql, AutoGenKey generatedKeys) {
        return setSql(String.join(" ", sql), generatedKeys);
    }

    public SqlQuery<T> setMapper(MapperConfig<T> config) {
        this.config = config;
        return this;
    }

    public SqlQuery<T> setTimeZoneOffset(ZoneOffset zoneOffset) {
        super.setZoneOffset(zoneOffset);
        return this;
    }

    /**
     * Devuelve el objeto query con formato
     *
     * @return (String) "Query(query)"
     */
    @Override
    String whoIam() {
        return String.format("Query(%s)", query);
    }

    /**
     * Agrega un nuevo parámetro definido con la sintaxis ":parameter"
     *
     * @param name  Nombre del parámetro. Sin ":" (key)
     * @param value Valor del parámetro (value) <br>
     *              Una instancia de {@link Date} se considera {@link java.sql.Timestamp} (fecha y hora), localizado
     *              en {@link #zoneOffset} (por defecto UTC, modificable con {@link #setTimeZoneOffset(ZoneOffset)}),
     *              salvo se indique usando el método {@link #dateToTemporal(Date, JDBCType)}<br>
     *              - Un objeto {@link java.time.LocalDate} para tomar sólo la fecha<br>
     *              - Un objeto {@link java.time.LocalTime} para tomar sólo la hora<br>
     * @return El mismo objeto SqlQuery
     */
    public SqlQuery<T> addParameter(String name, Object value) {
        if (name != null && value != null) {
            if (value instanceof Date) {
                value = super.dateToTemporal((Date) value, null);
            }
            parameters.put(name, value);
        } else if (nullParameter == null) {
            nullParameter = Optional.ofNullable(name);
        }
        return this;
    }

    private int getIndexWordSeparator(String str) {
        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            if (c == ',' || c == ' ' || c == ')' || c == '\n' || c == '\r' || c == ';') {
                return i;
            }
        }
        return str.length();
    }

    private String prepareQuery() {
        String[] queryParts = query.split(":");
        StringBuilder preparedQuery = new StringBuilder(queryParts[0]);
        for (int i = 1; i < queryParts.length; i++) {
            String queryPart = queryParts[i];
            int indexWordSeparator = this.getIndexWordSeparator(queryPart);
            String paramName = queryPart.substring(0, indexWordSeparator);
            parameterNames.add(paramName);
            String part = queryPart.substring(indexWordSeparator, queryPart.length());
            preparedQuery.append("?").append(part);
        }
        return preparedQuery.toString();
    }

    private void checkParameters() throws Exception {
        if (nullParameter != null) {
            String msg = nullParameter.orElse("").trim();
            msg = String.format("Parameter%s can't be null", msg.isEmpty() ? "" : (" '" + msg + "'"));
            throw Adapter.getException(whoIam(), msg);
        }
        for (String parameterName : parameterNames) {
            if (!parameters.keySet().contains(parameterName)) {
                String msg = String.format("Parameter '%s' is undefined", parameterName);
                throw Adapter.getException(whoIam(), msg);
            }
        }
    }

    private void registerParameters(PreparedStatement statement) throws Exception {
        for (int i = 0; i < parameterNames.size(); i++) {
            String name = parameterNames.get(i);
            Object value = parameters.get(name);
            int index = i + 1;
            try {
                Class objClass = value.getClass();
                if (objClass.isArray()) {
                    this.registerArrayParameter(statement, index, value);
                } else if (objClass == Integer.class) {
                    statement.setInt(index, (Integer) value);
                } else if (objClass == String.class) {
                    statement.setString(index, (String) value);
                } else if (objClass == Boolean.class) {
                    statement.setBoolean(index, (Boolean) value);
                } else if (objClass == Double.class) {
                    statement.setDouble(index, (Double) value);
                } else if (objClass == Float.class) {
                    statement.setFloat(index, (Float) value);
                } else if (value instanceof Temporal) {
                    super.setTemporal(statement, index, value, objClass);
                } else if (value instanceof InputStream) {
                    statement.setBlob(index, (InputStream) value);
                }
            } catch (Exception e) {
                throw Adapter.getException(e, this.whoIam(), name, value.toString());
            }
        }
    }

    private void registerArrayParameter(PreparedStatement statement, int index, Object obj) throws Exception {
        Class componentType = obj.getClass().getComponentType();
        if (componentType != null && componentType.isPrimitive()) {
            if (byte.class.isAssignableFrom(componentType)) {
                statement.setBytes(index, (byte[]) obj);
            }
        }
    }

    private PreparedStatement getStatement(String preparedQuery) throws Exception {
        try {
            return returnGeneratedKeys
                    ? connection.prepareStatement(preparedQuery, Statement.RETURN_GENERATED_KEYS)
                    : connection.prepareStatement(preparedQuery);
        } catch (SQLException e) {
            throw Adapter.getException(e, preparedQuery);
        }
    }

    private PreparedStatement executeStatement() throws Exception {
        Adapter.checkNotNullOrEmpty(query, "Query can't be null or empty");
        this.checkConnection();
        String preparedQuery = this.prepareQuery();
        this.checkParameters();
        PreparedStatement statement = this.getStatement(preparedQuery);
        this.registerParameters(statement);
        try {
            statement.execute();
            return statement;
        } catch (SQLException e) {
            throw Adapter.getException(e, this.whoIam());
        }
    }

    /**
     * Ejectua el query. No controla Excepciones. Si<br>
     * <code>autoCloseConnection == true</code> cierra la conexión (def: true)
     *
     * @param executor Ver implementación de un {@link Executor}
     * @return Devuelve un tipo de dato T
     * @throws Exception Si T es instancia de List o si hay error al ejecutar
     */
    public T execute(Executor<T> executor) throws Exception {
        Adapter.checkNotNull(executor, whoIam(), "The object executor can't be null");
        try {
            T result = executor.execute(this.executeStatement().getResultSet());
            Adapter.checkIsNotList(result, this.whoIam(), Adapter.getErrorTypeIsList());
            return result;
        } catch (Exception e) {
            throw Adapter.getException(e, this.whoIam());
        } finally {
            this.closeConnection();
        }
    }

    /**
     * Ejectua el query. No controla Excepciones. Si<br>
     * <code>autoCloseConnection == true</code> cierra la conexión (def: true)
     *
     * @param executor Ver implementación de un {@link ExecutorList}
     * @return Devuelve un tipo de dato T
     * @throws Exception Si hay error al ejecutar
     */
    public List<T> execute(ExecutorList<T> executor) throws Exception {
        Adapter.checkNotNull(executor, this.whoIam(), "The object executor can't be null");
        try {
            List<T> result = new ArrayList<>();
            executor.execute(this.executeStatement().getResultSet(), result);
            return result;
        } catch (Exception e) {
            throw Adapter.getException(e, this.whoIam());
        } finally {
            this.closeConnection();
        }
    }

    public T executeFirst() throws Exception {
        Adapter.checkNotNull(clazz, this.whoIam(), "Include the class object in constructor");
        Adapter.checkNotNull(config, this.whoIam(), "The object mapper is not set");
        return this.execute(rs -> {
            SqlMapper mapper = new SqlMapper(rs.getMetaData());
            T result = null;
            if (rs.next()) {
                result = clazz.newInstance();
                config.config(mapper, result, rs);
                mapper.apply();
            }
            return result;
        });
    }

    public List<T> executeList() throws Exception {
        Adapter.checkNotNull(clazz, this.whoIam(), "Include the class object in constructor");
        Adapter.checkNotNull(config, this.whoIam(), "The object mapper is not set");
        return this.execute((rs, list) -> {
            SqlMapper mapper = new SqlMapper(rs.getMetaData());
            while (rs.next()) {
                T result = clazz.newInstance();
                config.config(mapper, result, rs);
                mapper.apply();
                list.add(result);
            }
        });
    }

    /**
     * Ejectua el query. No controla Excepciones. Si<br>
     * <code>autoCloseConnection == true</code> cierra la conexión (def: true)
     * <br><br>
     * Se puede instanciar la clase como: <br>
     * <code>SqlQuery query = new SqlQuery();</code>
     *
     * @return rowCount/ID <b>rowCount</b><i>(default)</i>: Update count o -1 si
     * el resultado es un ResultSet o no hay más resultados. <br>
     * <b>ID<i>(Si GeneratedKeys.RETURN fue seleccionado)</i></b>: Primer ID
     * autogenerado a partir de un INSERT.
     * @throws Exception Si hay error al ejecutar
     */
    public int execute() throws Exception {
        if (returnGeneratedKeys) {
            ResultSet rs = executeStatement().getGeneratedKeys();
            if (rs.next()) {
                int autoGenId = Adapter.checkNotNullOrZero(rs.getInt(1), "Error al obtener ID autogenerado");
                closeConnection();
                return autoGenId;
            }
        }
        int rowCount = executeStatement().getUpdateCount();
        closeConnection();
        return rowCount;
    }

}

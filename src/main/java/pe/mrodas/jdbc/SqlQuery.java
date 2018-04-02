package pe.mrodas.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
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
 * @author Marco Rodas
 * @param <T> tipo devuelto por un método execute. Para {@link #execute()} o
 * {@link #executeResponse()} se ignora.
 */
public class SqlQuery<T> extends DBLayer {

    /**
     * Represent an Insert or Update operation
     */
    public interface Save {

        public Save addField(String name, Object value) throws Exception;

        public int execute() throws Exception;

        public boolean isInsert();

        public default boolean isUpdate() {
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
     * @author Marco Rodas
     * @param <T> tipo devuelto por la implementación de esta clase
     */
    public interface Executor<T> {

        T execute(ResultSet rs) throws Exception;

    }

    /**
     * Lectura de un ResultSet: <i>(Implementación)</i>
     * <pre><code>(ResultSet rs) -> {
     *  if (rs.next()) {
     *      obj.set...(rs.get...(...));
     *      obj.set...(rs.get...(...));
     *  }
     * }</code></pre>
     *
     * @author Marco Rodas
     * @param <T> tipo devuelto por la implementación de esta clase
     */
    public interface ExecutorClass<T> {

        void execute(ResultSet rs, T result) throws Exception;

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
     * @author Marco Rodas
     * @param <T> Tipo de elemento de la lista {@code List<T>} devuelta por esta
     * clase
     */
    public interface ExecutorList<T> {

        void execute(ResultSet rs, List<T> list) throws Exception;
    }

    public enum AutoGenKey {
        RETURN, NO_RETURN;
    }

    private String query;
    private boolean returnGeneratedKeys;
    private final HashMap<String, Object> parameters = new HashMap<>();
    private final List<String> parameterNames = new ArrayList<>();
    private Optional<String> nullParameter = null;

    public SqlQuery() {
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
     * @param name Nombre del parámetro. Sin ":" (key)
     * @param value Valor del parámetro (value) <br><pre>
     *  java.util.Date se considera TIMESTAMP (fecha y hora)
     *  - Use un objeto LocalDate para tomar sólo la fecha
     *  - Use un objeto LocalTime o Time para tomar sólo la hora</pre>
     *
     * @return El mismo objeto SqlQuery
     */
    public SqlQuery<T> addParameter(String name, Object value) {
        if (name == null || value == null) {
            if (nullParameter == null) {
                nullParameter = Optional.ofNullable(name);
            }
        } else {
            if (value.getClass() == Date.class) {
                Date date = (Date) value;
                value = new Timestamp(date.getTime());
            }
            parameters.put(name, value);
        }
        return this;
    }

    private int findWordSeparator(String str) {
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
        String preparedQuery = queryParts[0];
        for (int i = 1; i < queryParts.length; i++) {
            String queryPart = queryParts[i];
            int indexWordSeparator = findWordSeparator(queryPart);
            String paramName = queryPart.substring(0, indexWordSeparator);
            parameterNames.add(paramName);
            preparedQuery += "?" + queryPart.substring(indexWordSeparator, queryPart.length());
        }
        return preparedQuery;
    }

    private void checkParameters() throws Exception {
        if (nullParameter != null) {
            String msg = nullParameter.orElse("").isEmpty()
                    ? "No se debe ingresar un parámetro nulo"
                    : String.format("El parámetro %s no debe ser nulo", nullParameter.get());
            throw Adapter.getException(whoIam(), msg);
        }
        for (String parameterName : parameterNames) {
            if (!parameters.keySet().contains(parameterName)) {
                throw Adapter.getException(whoIam(), String.format("Falta agregar el parámetro '%s'", parameterName));
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
                    registerArrayParameter(statement, index, value);
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
                } else if (objClass == Time.class) {
                    statement.setTime(index, (Time) value);
                } else if (objClass == Timestamp.class) {
                    statement.setTimestamp(index, (Timestamp) value);
                } else if (objClass == LocalDate.class) {
                    statement.setDate(index, java.sql.Date.valueOf((LocalDate) value));
                } else if (objClass == LocalTime.class) {
                    statement.setTime(index, Time.valueOf((LocalTime) value));
                } else if (objClass == LocalDateTime.class) {
                    statement.setTimestamp(index, Timestamp.valueOf((LocalDateTime) value));
                }
            } catch (Exception e) {
                throw Adapter.getException(e, whoIam(), name, value.toString());
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
        PreparedStatement statement;
        try {
            statement = returnGeneratedKeys
                    ? connection.prepareStatement(preparedQuery, Statement.RETURN_GENERATED_KEYS)
                    : connection.prepareStatement(preparedQuery);
        } catch (Exception e) {
            throw Adapter.getException(e, preparedQuery);
        }
        return statement;
    }

    private PreparedStatement executeStatement() throws Exception {
        Adapter.checkNotNullOrEmpty(query, "El query no puede ser nulo o vacío");
        checkConnection();
        String preparedQuery = prepareQuery();
        checkParameters();
        PreparedStatement statement = getStatement(preparedQuery);
        registerParameters(statement);
        try {
            statement.execute();
        } catch (Exception e) {
            throw Adapter.getException(e, whoIam());
        }
        return statement;
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
        Adapter.checkNotNull(executor, whoIam(), "El objecto executor no puede ser nulo");
        PreparedStatement statement = executeStatement();
        T result = null;
        try {
            result = executor.execute(statement.getResultSet());
            Adapter.checkIsNotList(result, whoIam(), Adapter.getErrorTypeIsList());
        } catch (Exception e) {
            throw Adapter.getException(e, whoIam());
        } finally {
            closeConnection();
        }
        return result;
    }

    public T execute(Class<T> clazz, ExecutorClass<T> executor) throws Exception {
        Adapter.checkNotNull(executor, whoIam(), "El objecto executor no puede ser nulo");
        PreparedStatement statement = executeStatement();
        T result = clazz.newInstance();
        try {
            executor.execute(statement.getResultSet(), result);
            Adapter.checkIsNotList(result, whoIam(), Adapter.getErrorTypeIsList());
        } catch (Exception e) {
            throw Adapter.getException(e, whoIam());
        } finally {
            closeConnection();
        }
        return result;
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
        Adapter.checkNotNull(executor, whoIam(), "El objecto executor no puede ser nulo");
        PreparedStatement statement = executeStatement();
        List<T> result = new ArrayList<>();
        try {
            executor.execute(statement.getResultSet(), result);
        } catch (Exception e) {
            throw Adapter.getException(e, whoIam());
        } finally {
            closeConnection();
        }
        return result;
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

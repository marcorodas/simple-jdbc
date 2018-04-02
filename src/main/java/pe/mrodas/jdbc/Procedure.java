package pe.mrodas.jdbc;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javafx.util.Pair;

/**
 * Uso: <font color="yellow"><code>{@code
 * Procedure<Type> proc = new Procedure<>();}</code></font><br>
 * No usar: <font color="red"><code>{@code
 * Procedure<List<ElementType>> proc = new Procedure<>();}</code></font><br>
 * <i>(Excepción en los métodos call)</i><br>
 * Usar en cambio: <font color="yellow"><code>{@code
 * Procedure<ElementType> proc = new Procedure<>();}</code></font><br>
 * T obj = {@link #call(Executor)}<br>{@code List<T>} list =
 * {@link #call(ExecutorList)}<br>
 * Response obj = {@link #callResponse(Executor)} /T/<br>
 * Response obj = {@link #callResponse(ExecutorList)} /{@code List<T>}/
 *
 * @author Marco Rodas
 * @param <T> tipo devuelto por un método call. Para {@link #call()} o
 * {@link #callResponse()} se ignora.
 */
public class Procedure<T> extends DBLayer {

    /**
     * Lectura de un ResultSet: <i>(Implementación)</i>
     * <pre><code>(ResultSet rs) -> {
     *  T obj = new T(/.../);
     *  while (rs.next()) {
     *      /.../
     *  }
     *  return obj;
     * }</code></pre> Lectura de parámetros OUT: <i>(Implementación)</i>
     * <pre><code>(ResultSet rs) -> {
     *  CallableStatement statement = Procedure.getStatement(rs);
     *  statement.get***(/parameterOutName/)
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
     * }</code></pre> Lectura de parámetros OUT: <i>(Implementación)</i>
     * <pre><code>(ResultSet rs) -> {
     *  CallableStatement statement = Procedure.getStatement(rs);
     *  statement.get***(/parameterOutName/)
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
     * <pre><code>(ResultSet rs,{@code List<T> list}) -> {
     *  while (rs.next()) {
     *      list.add(obj);
     *  }
     * }</code></pre> Lectura de parámetros OUT: <i>(Implementación)</i>
     * <pre><code>(ResultSet rs,{@code List<T> list}) -> {
     *  CallableStatement statement = Procedure.getStatement(rs);
     *  statement.get***(/parameterOutName/)
     * }</code></pre>
     *
     * @author Marco Rodas
     * @param <T> Tipo de elemento de la lista {@code List<T>} devuelta por esta
     * clase
     */
    public interface ExecutorList<T> {

        void execute(ResultSet rs, List<T> list) throws Exception;
    }

    private String procedureName;
    private final HashMap<String, Pair<JDBCType, Object>> parametersIN = new HashMap<>();
    private final HashMap<String, JDBCType> parametersOUT = new HashMap<>();

    public Procedure() {
    }

    public Procedure(Connection connection, boolean autoCloseConnection) {
        super(connection, autoCloseConnection);
    }

    /**
     *
     * @param name Nombre del storedProcedure sin adicionales {}, CALL, "", etc
     * @return el mismo objeto Procedure
     */
    public Procedure<T> setName(String name) {
        procedureName = name;
        return this;
    }

    /**
     * Devuelve el nombre del procedure con formato
     *
     * @return (String) "Procedure(procedureName)"
     */
    @Override
    public String whoIam() {
        return String.format("Procedure(%s)", procedureName);
    }

    /**
     * Agrega un nuevo parametro tipo IN
     *
     * @param name Nombre del parametro (key)
     * @param value Valor del Parametro <br><pre>
     *  java.util.Date se considera TIMESTAMP
     *  (fecha y hora), salvo se indique
     *  - JDBCType.DATE para tomar sólo la fecha
     *  - JDBCType.TIME para tomar sólo la hora</pre>
     *
     * @param sqlType Tipo de parametro
     * @return El mismo objeto Procedure
     */
    public Procedure<T> addParameter(String name, Object value, JDBCType sqlType) {
        if (null != sqlType && value != null && value.getClass() == Date.class) {
            Date date = (Date) value;
            switch (sqlType) {
                case DATE:
                    value = new java.sql.Date(date.getTime());
                    break;
                case TIME:
                    value = new Time(date.getTime());
                    break;
                default:
                    value = new Timestamp(date.getTime());
                    break;
            }
        }
        parametersIN.put(name, new Pair<>(sqlType, value));
        return this;
    }

    /**
     * Agrega un nuevo parametro tipo OUT
     *
     * @param name Nombre del Parametro (key)
     * @param sqlType Tipo de parametro
     * @return El mismo objeto Procedure
     */
    public Procedure<T> addParameter(String name, JDBCType sqlType) {
        parametersOUT.put(name, sqlType);
        return this;
    }

    private void registerArrayParameter(CallableStatement statement, String name, Object obj) throws Exception {
        Class componentType = obj.getClass().getComponentType();
        if (componentType != null && componentType.isPrimitive()) {
            if (byte.class.isAssignableFrom(componentType)) {
                statement.setBytes(name, (byte[]) obj);
            }
        }
    }

    private void registerInParameter(CallableStatement statement, String name, Object value) throws Exception {
        Class objClass = value.getClass();
        if (objClass.isArray()) {
            registerArrayParameter(statement, name, value);
        } else if (objClass == Integer.class) {
            statement.setInt(name, (Integer) value);
        } else if (objClass == String.class) {
            statement.setString(name, (String) value);
        } else if (objClass == Boolean.class) {
            statement.setBoolean(name, (Boolean) value);
        } else if (objClass == Double.class) {
            statement.setDouble(name, (Double) value);
        } else if (objClass == Float.class) {
            statement.setFloat(name, (Float) value);
        } else if (objClass == java.sql.Date.class) {
            statement.setDate(name, (java.sql.Date) value);
        } else if (objClass == Time.class) {
            statement.setTime(name, (Time) value);
        } else if (objClass == Timestamp.class) {
            statement.setTimestamp(name, (Timestamp) value);
        } else if (objClass == LocalDate.class) {
            statement.setDate(name, java.sql.Date.valueOf((LocalDate) value));
        } else if (objClass == LocalTime.class) {
            statement.setTime(name, Time.valueOf((LocalTime) value));
        } else if (objClass == LocalDateTime.class) {
            statement.setTimestamp(name, Timestamp.valueOf((LocalDateTime) value));
        }
    }

    private String parameterToString(String name, JDBCType type, Object value, boolean isOUT) {
        String parameter = isOUT
                ? String.format("OUT, name:%s, type:%s", name, type)
                : (value == null
                        ? String.format("IN, name:%s, value:NULL, type:%s", name, type)
                        : String.format("IN, name:%s, value:%s", name, value));
        return String.format("Parameter(%s)", parameter);
    }

    private void registerParameters(CallableStatement statement) throws Exception {
        String name = null;
        JDBCType jdbcType = null;
        Object value = null;
        boolean isOUT = false;
        try {
            for (Map.Entry<String, Pair<JDBCType, Object>> entry : parametersIN.entrySet()) {
                name = entry.getKey();
                Pair<JDBCType, Object> pair = entry.getValue();
                value = pair.getValue();
                if (value == null) {
                    jdbcType = pair.getKey();
                    statement.setNull(name, jdbcType.getVendorTypeNumber());
                } else {
                    registerInParameter(statement, name, value);
                }
            }
            isOUT = true;
            for (Map.Entry<String, JDBCType> entry : parametersOUT.entrySet()) {
                name = entry.getKey();
                jdbcType = entry.getValue();
                statement.registerOutParameter(name, jdbcType.getVendorTypeNumber());
            }
        } catch (Exception e) {
            String parameter = parameterToString(name, jdbcType, value, isOUT);
            throw Adapter.getException(e, whoIam(), parameter);
        }
    }

    private CallableStatement getStatement() throws Exception {
        List<String> list = Collections.nCopies(parametersIN.size() + parametersOUT.size(), "?");
        String params = String.join(",", list);
        String call = String.format("{CALL %s(%s)}", procedureName, params);
        CallableStatement statement;
        try {
            statement = connection.prepareCall(call);
        } catch (Exception e) {
            throw Adapter.getException(e, call);
        }
        return statement;
    }

    private void checkProcedureName() throws Exception {
        Adapter.checkNotNullOrEmpty(procedureName, "El nombre del procedimiento no puede ser vacío o nulo");
        if (procedureName.split(" ").length > 1) {
            throw Adapter.getException(whoIam(), "El nombre del procedimiento no puede contener espacios");
        }
    }

    /**
     * Ejectua el procedure. No controla Excepciones. No cierra la conexión.
     *
     * @return CallableStatement
     * @throws Exception Si ocurre un error
     */
    protected CallableStatement executeStatement() throws Exception {
        checkProcedureName();
        checkConnection();
        CallableStatement statement = getStatement();
        registerParameters(statement);
        try {
            statement.execute();
        } catch (Exception e) {
            throw Adapter.getException(e, whoIam());
        }
        return statement;
    }

    /**
     * Ejectua el procedure. No controla Excepciones. Si<br>
     * <code>autoCloseConnection == true</code> cierra la conexión (def: true)
     *
     * @param executor Ver implementación de un {@link Executor}
     * @return Devuelve un tipo de dato T
     * @throws Exception Si T es instancia de List o si hay error al ejecutar
     */
    public T call(Executor<T> executor) throws Exception {
        Adapter.checkNotNull(executor, whoIam(), "El objecto executor no puede ser nulo");
        CallableStatement statement = executeStatement();
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

    public T call(Class<T> clazz, ExecutorClass<T> executor) throws Exception {
        Adapter.checkNotNull(executor, whoIam(), "El objecto executor no puede ser nulo");
        CallableStatement statement = executeStatement();
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
     * Ejectua el procedure. No controla Excepciones. Si<br>
     * <code>autoCloseConnection == true</code> cierra la conexión (def: true)
     *
     * @param executor Ver implementación de un {@link ExecutorList}
     * @return Devuelve un {@code List<T>}
     * @throws Exception Si hay error al ejecutar
     */
    public List<T> call(ExecutorList<T> executor) throws Exception {
        Adapter.checkNotNull(executor, whoIam(), "El objecto executor no puede ser nulo");
        CallableStatement statement = executeStatement();
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
     * Ejectua el procedure. No controla Excepciones. Si<br>
     * <code>autoCloseConnection == true</code> cierra la conexión (def: true)
     * <br><br>
     * Se puede instanciar la clase como: <br>
     * <code>Procedure procedure = new Procedure();</code>
     *
     * @throws Exception Si hay error al ejecutar
     */
    public void call() throws Exception {
        try {
            executeStatement();
        } finally {
            closeConnection();
        }
    }

    public static CallableStatement getStatement(ResultSet rs) throws Exception {
        Statement statement = rs.getStatement();
        return statement == null ? null : (CallableStatement) statement;
    }
}

package pe.mrodas.jdbc;

import java.io.File;
import java.lang.reflect.Array;
import java.nio.file.Path;
import java.sql.Connection;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author Marco Rodas
 */
public class Adapter {

    public interface Batch {

        void execute(Connection connection) throws Exception;

    }

    public interface BatchGet<T> {

        T execute(Connection connection) throws Exception;

    }

    public interface Runner {

        void execute() throws Exception;
    }

    public interface ExecutorList<T> {

        List<T> execute() throws Exception;
    }

    private static final String ERROR_TYPE_IS_LIST = String.join(" ", new String[]{
            "No use: Procedure<List<ElementType>> proc = new Procedure<>();",
            "- Use en cambio: Procedure<ElementType> proc = new Procedure<>();",
            String.format("(e:%s)", "Message body writer NOT found")
    });

    public static String getErrorTypeIsList() {
        return ERROR_TYPE_IS_LIST;
    }

    public static String getPropertyAsString(Properties prop) {
        return prop.entrySet().stream()
                .map(entry -> String.format("%s = %s", entry.getKey(), entry.getValue()))
                .collect(Collectors.joining("; "));
    }

    protected static <T> T throwIfCondition(Function<T, Boolean> condition, T obj, String... msgs) throws Exception {
        if (condition.apply(obj)) {
            String error = Adapter.getCaller();
            error += msgs == null ? "" : (": " + String.join("-", msgs));
            throw new IllegalArgumentException(error);
        }
        return obj;
    }

    public static <T> T checkNotNull(T obj, String... msgs) throws Exception {
        return throwIfCondition(testObj -> testObj == null, obj, msgs);
    }

    public static Integer checkNotNullOrZero(Integer value, String... msgs) throws Exception {
        return throwIfCondition(testObj -> testObj == null || testObj == 0, value, msgs);
    }

    public static String checkNotNullOrEmpty(String str, String... msgs) throws Exception {
        return throwIfCondition(testObj -> testObj == null || testObj.isEmpty(), str, msgs);
    }

    public static <T> List<T> checkNotNullOrEmpty(List<T> list, String... msgs) throws Exception {
        return throwIfCondition(testObj -> testObj == null || testObj.isEmpty(), list, msgs);
    }

    public static Object checkIsNotList(Object object, String... msgs) throws Exception {
        return throwIfCondition(testObj -> testObj instanceof List<?>, object, msgs);
    }

    public static File checkFileExists(File file, String... msgs) throws Exception {
        return throwIfCondition(testObj -> !testObj.exists(), file, msgs);
    }

    public static Path checkFileExists(Path path, String... msgs) throws Exception {
        return throwIfCondition(testObj -> !testObj.toFile().exists(), path, msgs);
    }

    public static Exception getException(Exception ex) {
        return getException(ex, "");
    }

    public static Exception getException(String... msgs) {
        return getException(null, msgs);
    }

    private static boolean isLibraryEnclosedClass(Class oClass) {
        Class enclosingClass = oClass.getEnclosingClass();
        return enclosingClass != null && isLibraryClass(enclosingClass);
    }

    private static boolean isLibraryClass(Class oClass) {
        return Adapter.class.isAssignableFrom(oClass) || DBLayer.class.isAssignableFrom(oClass);
    }

    private static boolean isLibraryElement(StackTraceElement traceElement) {
        Class oClass = traceElement.getClass();
        return isLibraryClass(oClass) || isLibraryEnclosedClass(oClass);
    }

    private static String getSimpleClassName(StackTraceElement traceElement) {
        String name = traceElement.getClassName();
        return name.substring(name.lastIndexOf(".") + 1);
    }

    private static String getCaller() {
        StackTraceElement[] traceElements = Thread.currentThread().getStackTrace();
        int i = 0;
        for (StackTraceElement traceElement : traceElements) {
            if (i > 1 && !isLibraryElement(traceElement)) {
                String method = traceElement.getMethodName();
                return String.format("%s.%s()", getSimpleClassName(traceElement), method);
            }
            i++;
        }
        return null;
    }

    public static Exception getException(Exception ex, String... msgs) {
        List<String> list = new ArrayList<>();
        if (msgs != null) {
            list.add(String.join(" - ", msgs));
        }
        if (ex != null) {
            list.add(String.format("(e:%s)", ex.getMessage()));
        }
        String caller = getCaller();
        if (caller != null) {
            list.add(0, list.isEmpty() ? caller : (caller + ":"));
        }
        return new Exception(String.join(" ", list));
    }


    public static Connection getConnection() throws Exception {
        return DBLayer.Connector.getInstance().getConnection();
    }

    /**
     * Ejecuta el método batch.execute(connection). Se brinda una conexión a la
     * base de datos. La conexión se cierra automáticamente. No cerrar
     * manualmente. Si hay algún error realiza un rollback automático.
     *
     * @param batch
     * @throws Exception
     */
    public static void batch(Batch batch) throws Exception {
        try (Connection connection = getConnection()) {
            connection.setAutoCommit(false);
            try {
                batch.execute(connection);
                connection.commit();
            } catch (Exception e) {
                connection.rollback();
                throw e;
            }
        }
    }

    /**
     * Ejecuta el método batch.execute(connection). Se brinda una conexión a la
     * base de datos. La conexión se cierra automáticamente. No cerrar
     * manualmente. Si hay algún error realiza un rollback automático.
     *
     * @param batch
     * @throws Exception
     */
    public static <T> T batch(BatchGet<T> batch) throws Exception {
        try (Connection connection = getConnection()) {
            connection.setAutoCommit(false);
            try {
                T result = batch.execute(connection);
                connection.commit();
                return result;
            } catch (Exception e) {
                connection.rollback();
                throw e;
            }
        }
    }

    @Deprecated
    @SuppressWarnings("unchecked")
    public static <E> E[] listToArray(List<E> list) {
        if (list.isEmpty()) {
            return null;
        } else {
            Class<?> objClass = list.get(0).getClass();
            E[] genericNewArray = (E[]) Array.newInstance(objClass, list.size());
            return list.toArray(genericNewArray);
        }
    }

    public static List<Integer> parseIdStringList(String strList, String separator) {
        String[] idArray = Optional.ofNullable(strList).orElse("").trim().split(Optional.ofNullable(separator).orElse(","));
        return Arrays.stream(idArray)
                .filter(Adapter::onlyDigits)
                .map(item -> Integer.parseInt(item.trim()))
                .collect(Collectors.toList());
    }

    public static boolean onlyDigits(String digits) {
        return digits != null && !digits.trim().isEmpty() && digits.trim().matches("\\d+");
    }

    public static Integer parseInt(String integer, Integer valueIfUnableToParse) {
        return onlyDigits(integer) ? Integer.parseInt(integer.trim()) : valueIfUnableToParse;
    }

    public static <K, V> K getMapKeyByTest(Map<K, V> map, Function<V, Boolean> testCondition) {
        for (Map.Entry<K, V> entry : map.entrySet()) {
            V testValue = entry.getValue();
            if (testCondition.apply(testValue)) {
                return entry.getKey();
            }
        }
        return null;
    }

    public static <K, V> K getMapKey(Map<K, V> map, V value) {
        return getMapKeyByTest(map, testValue -> testValue == value);
    }

    private static ZonedDateTime getZonedDateTime(Date date) {
        return date.toInstant().atZone(ZoneId.systemDefault());
    }

    public static LocalDate getLocalDate(Date date) {
        return date == null ? null : getZonedDateTime(date).toLocalDate();
    }

    public static LocalTime getLocalTime(Date date) {
        return date == null ? null : getZonedDateTime(date).toLocalTime();
    }

    public static LocalDateTime getLocalDateTime(Date date) {
        return date == null ? null : getZonedDateTime(date).toLocalDateTime();
    }

}

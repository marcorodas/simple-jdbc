package pe.mrodas.helper;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.fasterxml.uuid.Generators;

/**
 * Convenience class to handle IN operator in WHERE clause.
 * Designed to work with a list or stream of INTEGER ids.<br><br>
 * Sample Use: <br><code>{@code
 * SqlInOperator<Integer> inList = new SqlInOperator<>(listOfIntegers);}</code><br><code>{@code
 * String sql = "SELECT field FROM table WHERE intField IN" + inList;}</code><br><code>{@code
 * SqlQuery<SampleClass> query = new SqlQuery<>().setSql(sql);}</code><br><code>{@code
 * [...]}</code><br><code>{@code
 * inList.getParameters().forEach(query::addParameter);}</code><br><code>{@code
 * [...]query.execute...[...]
 * }</code>
 *
 * @author Marco Rodas
 */
public class SqlInOperator<T> {

    private String fields;
    private final Map<String, T> parameters;

    public SqlInOperator(List<T> list) {
        this(list == null ? null : list.stream());
    }

    /**
     * Initilize the object accumulating the Stream into a List. Null elements are filtered.
     * The stream can no longer be used.
     *
     * @param ids Stream pipeline to be consumed as List
     */
    public SqlInOperator(Stream<T> ids) {
        if (ids == null) parameters = null;
        else {
            List<T> list = ids.filter(Objects::nonNull).collect(Collectors.toList());
            if (list.isEmpty()) parameters = null;
            else {
                String prefix = "id" + Generators.timeBasedGenerator().generate().clockSequence();
                if (list.size() == 1) {
                    fields = ":" + prefix + "0";
                    parameters = Collections.singletonMap(prefix + "0", list.get(0));
                } else {
                    fields = IntStream.range(0, list.size())
                            .mapToObj(i -> ":" + prefix + i)
                            .collect(Collectors.joining(","));
                    parameters = new HashMap<>();
                    for (int i = 0; i < list.size(); i++)
                        parameters.put(prefix + i, list.get(i));
                }
            }
        }
    }

    @Override
    public String toString() {
        return String.format("(%s)", fields);
    }

    public Map<String, T> getParameters() {
        return isInvalid() ? Collections.emptyMap() : parameters;
    }

    /**
     * Check for incorrect initialization.
     *
     * @return true if the initial list is null or empty, false otherwise
     */
    public boolean isInvalid() {
        return parameters == null;
    }
}

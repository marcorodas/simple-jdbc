package pe.mrodas.helper;

import com.fasterxml.uuid.Generators;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

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

    private final String prefix = "id" + Generators.timeBasedGenerator().generate().clockSequence();
    private final String fields;
    private final Map<String, T> parameters = new HashMap<>();

    public SqlInOperator(List<T> list) {
        fields = IntStream
                .range(0, list.size())
                .mapToObj(i -> ":" + prefix + i)
                .collect(Collectors.joining(","));
        for (int i = 0; i < list.size(); i++) {
            parameters.put(prefix + i, list.get(i));
        }
    }

    public SqlInOperator(Stream<T> ids) {
        this(ids.collect(Collectors.toList()));
    }

    @Override
    public String toString() {
        return String.format("(%s)", fields);
    }

    public Map<String, T> getParameters() {
        return parameters;
    }
}

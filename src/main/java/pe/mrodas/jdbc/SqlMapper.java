package pe.mrodas.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author skynet
 */
public class SqlMapper {

    public interface Setter<T> {

        void set(T value);
    }

    public interface Getter<T> {

        T get(String columnName) throws SQLException;
    }

    private final List<String> columnNames = new ArrayList<>();
    private final List<Setter> setterList = new ArrayList<>();
    private final List<Getter> getterList = new ArrayList<>();

    public SqlMapper(ResultSetMetaData rsmd) throws SQLException {
        for (int i = 1; i <= rsmd.getColumnCount(); i++) {
            columnNames.add(rsmd.getColumnLabel(i));
        }
    }

    public <T> SqlMapper map(Setter<T> setter, Getter<T> getter) throws Exception {
        Adapter.checkNotNull(setter, "Setter can't be null");
        Adapter.checkNotNull(getter, "Getter can't be null");
        setterList.add(setter);
        getterList.add(getter);
        return this;
    }

    @SuppressWarnings("unchecked")
    public void apply() throws SQLException {
        for (int i = 0; i < setterList.size(); i++) {
            String columnName = columnNames.get(i);
            Object obj = getterList.get(i).get(columnName);
            setterList.get(i).set(obj);
        }
        setterList.clear();
        getterList.clear();
    }

}

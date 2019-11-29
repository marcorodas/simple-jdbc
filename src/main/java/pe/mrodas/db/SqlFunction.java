package pe.mrodas.db;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import pe.mrodas.db.helper.CursorIterator;
import pe.mrodas.db.helper.ThrowingBiFunction;

public class SqlFunction<T> {
    private final static String QUERY = "SELECT <function>(<parameters>) value";
    private final List<Object> parameters = new ArrayList<>();
    private final String name;
    private String error;

    public SqlFunction(String name) {
        this.name = name;
    }

    public SqlFunction<T> addParameter(Object parameter) {
        if (error != null) return this;
        if (parameter == null)
            error = String.format("Parameter #%s value can't be null!", parameters.size());
        else parameters.add(parameter);
        return this;
    }

    public T execute(ThrowingBiFunction<ResultSet, String, T> mapper) throws IOException, SQLException {
        return this.execute(null, mapper);
    }

    public T execute(Connection connection, ThrowingBiFunction<ResultSet, String, T> mapper) throws IOException, SQLException {
        if (name == null) throw new IOException("Function name can't be null!");
        if (error != null) throw new IOException(error);
        int numParameters = parameters.size();
        List<String> params = Collections.nCopies(numParameters, "?");
        String preparedQuery = QUERY.replace("<function>", name)
                .replace("<parameters>", String.join(", ", params));
        Connection conn = connection == null ? Connector.getConnection() : connection;
        PreparedStatement statement = conn.prepareStatement(preparedQuery);
        for (Integer pos : new CursorIterator(numParameters)) {
            String name = String.format("#%s", pos);
            Object value = parameters.get(pos);
            SqlQuery.tryRegisterParameter(statement, pos, name, value);
        }
        statement.execute();
        ResultSet rs = statement.getResultSet();
        if (rs.next()) try {
            return mapper.apply(rs, "value");
        } catch (Exception e) {
            throw new IOException("Mapping Error: " + e.getMessage(), e);
        } finally {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return null;
    }
}

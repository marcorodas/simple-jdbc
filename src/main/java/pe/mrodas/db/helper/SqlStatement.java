package pe.mrodas.db.helper;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import pe.mrodas.db.Connector;

public abstract class SqlStatement<T> {

    private Connection connection;
    private Autoclose autoclose;

    public SqlStatement(Connection connection, Autoclose autoclose) {
        this.connection = connection;
        this.autoclose = autoclose;
    }

    public SqlStatement() {
        this.autoclose = Autoclose.YES;
    }

    public Connection getConnection() throws IOException, SQLException {
        if (connection == null) connection = Connector.getConnection();
        return connection;
    }

    protected abstract SQLException buildCallableException(SQLException e);

    protected abstract PreparedStatement executeStatement() throws IOException, SQLException;

    protected T run(Callable<T> callable) throws SQLException, IOException {
        try {
            return callable.call();
        } catch (SQLException e) {
            throw this.buildCallableException(e);
        } catch (Exception e) {
            throw new IOException("Mapping Error: " + e.getMessage(), e);
        } finally {
            this.close();
        }
    }

    protected List<T> runForList(Callable<List<T>> callable) throws SQLException, IOException {
        try {
            return callable.call();
        } catch (SQLException e) {
            throw this.buildCallableException(e);
        } catch (Exception e) {
            throw new IOException("Mapping Error: " + e.getMessage(), e);
        } finally {
            this.close();
        }
    }

    public T execute(Supplier<T> objGenerator, ThrowingBiConsumer<T, ResultSet> mapper) throws IOException, SQLException {
        T obj = objGenerator.get();
        PreparedStatement statement = this.executeStatement();
        ResultSet rs = statement.getResultSet();
        return this.run(() -> {
            if (rs.next()) mapper.accept(obj, rs);
            return obj;
        });
    }

    public T execute(ThrowingBiFunction<PreparedStatement, ResultSet, T> executor) throws IOException, SQLException {
        PreparedStatement statement = this.executeStatement();
        ResultSet rs = statement.getResultSet();
        return this.run(() -> executor.apply(statement, rs));
    }

    public List<T> executeForList(Supplier<T> objGenerator, ThrowingBiConsumer<T, ResultSet> mapper) throws IOException, SQLException {
        List<T> list = new ArrayList<>();
        PreparedStatement statement = this.executeStatement();
        ResultSet rs = statement.getResultSet();
        return this.runForList(() -> {
            while (rs.next()) {
                T obj = objGenerator.get();
                mapper.accept(obj, rs);
                list.add(obj);
            }
            return list;
        });
    }

    public List<T> executeForList(ThrowingBiFunction<PreparedStatement, ResultSet, List<T>> executor) throws IOException, SQLException {
        PreparedStatement statement = this.executeStatement();
        ResultSet rs = statement.getResultSet();
        return this.runForList(() -> executor.apply(statement, rs));
    }

    protected void close() {
        if (this.autoclose == Autoclose.YES) try {
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}

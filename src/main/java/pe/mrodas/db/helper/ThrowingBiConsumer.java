package pe.mrodas.db.helper;

import java.sql.SQLException;

@FunctionalInterface
public interface ThrowingBiConsumer<T, U> {
    void accept(T t, U u) throws SQLException;
}

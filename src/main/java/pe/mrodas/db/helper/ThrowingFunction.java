package pe.mrodas.db.helper;

@FunctionalInterface
public interface ThrowingFunction<T, R> {
    R apply(T arg) throws Exception;
}
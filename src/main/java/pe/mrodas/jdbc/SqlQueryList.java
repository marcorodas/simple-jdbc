package pe.mrodas.jdbc;

import java.sql.ResultSet;
import java.util.List;
import java.util.function.Supplier;

import pe.mrodas.helper.SqlInOperator;

public class SqlQueryList<T> extends SqlQuery<T> {

    public interface Mapper<T> {
        void map(ResultSet rs, T item) throws Exception;
    }

    /**
     * Establece el query. Reemplaza los caracteres comodín <tt>'?'</tt> por el parámetro <tt>inOperator</tt> usando el método {@link SqlInOperator#toString()} .
     * Agrega automáticamente los valores del parámetro <tt>inOperator</tt> al objeto {@link SqlQuery} usando los métodos
     * {@link SqlInOperator#getParameters()} y {@link SqlQuery#addParameter(String, Object)}
     * Uso:
     * <pre><code>
     * query.setSql(new String[]{
     *  "SELECT column1, column2",
     *  "   FROM table",
     *  "   WHERE column2 = :column2",
     *  "   AND column1 IN ?"
     * }, inOperator);
     * </code></pre>
     *
     * @return El mismo objeto SqlQuery
     */
    public SqlQueryList<T> setSql(String[] sql, SqlInOperator<?> inOperator) {
        inOperator.getParameters().forEach(super::addParameter);
        super.setSql(String.join(" ", sql).replace("?", inOperator.toString()));
        return this;
    }

    public List<T> execute(Supplier<T> itemFactory, Mapper<T> mapper) throws Exception {
        return super.execute((rs, list) -> {
            while (rs.next()) {
                T item = itemFactory.get();
                mapper.map(rs, item);
                list.add(item);
            }
        });
    }
}

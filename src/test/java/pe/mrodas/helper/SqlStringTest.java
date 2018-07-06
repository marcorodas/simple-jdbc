package pe.mrodas.helper;

import org.junit.Test;

import java.io.IOException;

public class SqlStringTest {

    @Test
    public void toStringArray() throws IOException {
        String path = "C:\\Users\\skynet\\.IdeaIC2018.1\\config\\scratches\\file-collection-rest-curl\\query.sql";
        System.out.println(new SqlString(path).toStrArray());
    }

    @Test
    public void toSql() throws IOException {
        String path = "C:\\Users\\skynet\\.IdeaIC2018.1\\config\\scratches\\file-collection-rest-curl\\query.txt";
        System.out.println(new SqlString(path).toSql());
    }
}
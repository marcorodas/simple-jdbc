package pe.mrodas.helper;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Convenience class to handle 2 situations:
 * 1. Converts .sql to String[] for SqlQuery<...>.setSql(new String[]{...})
 * 2. Converts String[] to .sql
 */
public class SqlString {

    private final Stream<String> stream;

    public SqlString(String file) throws InvalidPathException, IOException {
        Path pathFile = Paths.get(file);
        stream = Files.readAllLines(pathFile, Charset.defaultCharset())
                .stream();
    }

    public String toStrArray() {
        return stream.map(s -> "\"" + s + "\"")
                .collect(Collectors.joining(",\n"));
    }

    public String toSql() {
        return stream.map(s -> {
            String line = s.trim();
            return line.substring(1, line.length() - 2);
        }).collect(Collectors.joining("\n"));
    }
}

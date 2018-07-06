package pe.mrodas.helper;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Convenience class to handle 2 situations:
 * 1. Converts .sql to String[] for SqlQuery<...>.setSql(new String[]{...})
 * 2. Converts String[] to .sql
 */
public class SqlString {

    private final List<String> lines;

    public SqlString(String file) throws InvalidPathException, IOException {
        this.lines = Files.readAllLines(Paths.get(file), Charset.defaultCharset());
    }

    public String toStrArray() {
        return lines.stream().map(s -> "\"" + s + "\"")
                .collect(Collectors.joining(",\n"));
    }

    public String toSql() {
        List<String> sqlLines = new ArrayList<>();
        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i).trim();
            int endIndex = i == lines.size() - 1 ? 1 : 2;
            sqlLines.add(line.substring(1, line.length() - endIndex));
        }
        return sqlLines.stream().collect(Collectors.joining("\n"));
    }
}

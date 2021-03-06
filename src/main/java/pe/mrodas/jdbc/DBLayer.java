package pe.mrodas.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.Temporal;
import java.util.Calendar;
import java.util.Date;
import java.util.Optional;
import java.util.Properties;
import java.util.TimeZone;

/**
 * @author Marco Rodas
 */
public abstract class DBLayer {

    public static class Connector {

        private static Connector instance;
        private static String[] connectionStringParts;
        private static String propertyFile;
        private final String url;
        private final Properties dbInfo;

        private Connector() throws Exception {
            dbInfo = getDbProperties();
            url = dbInfo.getProperty("url");
            Class.forName(dbInfo.getProperty("driver"));
        }

        private Properties getDbProperties() throws IOException {
            Properties properties = new Properties();
            if (Optional.ofNullable(propertyFile).orElse("").isEmpty()) {
                for (String part : connectionStringParts) {
                    if (part.contains("=")) {
                        String[] property = part.split("=");
                        properties.setProperty(property[0].trim(), property[1].trim());
                    }
                }
            } else {
                ClassLoader cl = getClass().getClassLoader();
                try (InputStream file = cl.getResourceAsStream(propertyFile)) {
                    properties.load(file);
                }
            }
            return properties;
        }

        /**
         * Sample Use:
         * <pre>
         * {@code
         * Connector.configure(new String[]{
         *      "url = jdbc:mysql://127.0.0.1:3306/dbName",
         *      "driver = com.mysql.jdbc.Driver",
         *      "user = userName",
         *      "password = pass"
         * });
         * }</pre>
         *
         * @param connectionStringParts Propiedad: "key = value"
         */
        public static void configure(String[] connectionStringParts) {
            instance = null;
            propertyFile = null;
            Connector.connectionStringParts = connectionStringParts;
        }

        /**
         * connectionString:
         * <pre>
         * {@code
         * "url = jdbc:mysql://127.0.0.1:3306/dbName; driver = com.mysql.jdbc.Driver; user = userName; password = pass"
         * }</pre>
         *
         * @param connectionString
         */
        public static void configure(String connectionString) {
            configure(connectionString.split(";"));
        }

        /**
         * Properties:
         * <pre>
         * {@code
         *  url = jdbc\:mysql\://127.0.0.1:3306/sgsi
         *  driver = com.mysql.jdbc.Driver
         *  user = userName
         *  password = pass
         * });
         * }</pre>
         *
         * @param properties
         */
        public static void configure(Properties properties) {
            configure(Adapter.getPropertyAsString(properties));
        }

        /**
         * Archivo ubicado generalmente en src\main\resources
         * <p>
         * Debe contener las siguientes propiedades para la conexión:</p>
         * <pre>
         * {@code
         * url=jdbc:mysql://127.0.0.1:3306/dbName
         * driver=com.mysql.jdbc.Driver
         * user=userName
         * password=pass
         * }</pre>
         *
         * @param propertyFile Nombre de archivo de propiedades ej:
         *                     "db.properties"
         */
        public static void configureWithPropFile(String propertyFile) {
            instance = null;
            connectionStringParts = null;
            Connector.propertyFile = propertyFile;
        }

        /**
         * Obtiene una conexión a la base de datos. <br>
         * Se define con el método configure() o configureWithPropFile()
         *
         * @return Connection
         * @throws Exception Si no se puede leer el archivo de propiedades o no
         *                   se puede conectar
         */
        public Connection getConnection() throws Exception {
            return DriverManager.getConnection(url, dbInfo);
        }

        public static Connector getInstance() throws Exception {
            if (instance == null) {
                if (propertyFile == null && connectionStringParts == null) {
                    throw Adapter.getException("Configure con el método Connector.configure() o Connector.configureWithPropFile() (e:Connector no configurado)");
                }
                instance = new Connector();
                connectionStringParts = null;
            }
            return instance;
        }
    }

    protected Connection connection = null;
    private final boolean connectionProvided;
    private boolean autoCloseConnection = true;
    private ZoneOffset zoneOffset = ZoneOffset.UTC;
    private Calendar calendar = this.getCalendar(zoneOffset);

    public DBLayer(Connection connection, boolean autoCloseConnection) {
        this.connection = connection;
        this.autoCloseConnection = autoCloseConnection;
        connectionProvided = true;
    }

    public DBLayer() {
        connectionProvided = false;
    }

    abstract String whoIam();

    protected void checkConnection() throws Exception {
        if (connectionProvided) {
            if (connection == null) {
                throw Adapter.getException(whoIam(), "La conexión brindada no puede ser nula");
            } else if (connection.isClosed()) {
                throw Adapter.getException(whoIam(), "La conexión brindada está cerrada");
            }
        } else if (connection == null) {
            connection = Connector.getInstance().getConnection();
        }
    }

    protected void closeConnection() {
        if (autoCloseConnection) {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException ex) {
                System.err.println(ex);
            }
        }
    }

    private Calendar getCalendar(ZoneOffset zoneOffset) {
        ZoneId zoneId = ZoneId.ofOffset("", zoneOffset);
        return Calendar.getInstance(TimeZone.getTimeZone(zoneId));
    }

    void setZoneOffset(ZoneOffset zoneOffset) {
        this.zoneOffset = zoneOffset;
        this.calendar = this.getCalendar(zoneOffset);
    }

    private OffsetDateTime getOffsetDateTime(Date date) {
        return Instant.ofEpochMilli(date.getTime()).atOffset(zoneOffset);
    }

    /**
     * Devuelve una fecha localizada en {@link #zoneOffset} por defecto es <code>UTC</code>.
     *
     * @param date    fecha a convertir
     * @param sqlType Indicador de conversión<br>
     *                <code>JDBCType.DATE</code> para {@link java.time.LocalDate}<br>
     *                <code>JDBCType.TIME</code> para {@link java.time.LocalTime}<br>
     *                Cualquier otro valor para {@link java.time.LocalDateTime}
     * @return Un objeto {@link java.time.temporal.Temporal} según el parámetro <code>sqlType</code>
     */
    public Temporal dateToTemporal(Date date, JDBCType sqlType) {
        if (sqlType == JDBCType.DATE) {
            return this.getOffsetDateTime(date).toLocalDate();
        } else if (sqlType == JDBCType.TIME) {
            return this.getOffsetDateTime(date).toLocalTime();
        } else {
            return this.getOffsetDateTime(date).toLocalDateTime();
        }
    }

    void setTemporal(PreparedStatement statement, int index, Object value, Class objClass) throws SQLException {
        if (objClass == LocalDateTime.class) {
            statement.setTimestamp(index, Timestamp.valueOf((LocalDateTime) value), calendar);
        } else if (objClass == LocalDate.class) {
            statement.setDate(index, java.sql.Date.valueOf((LocalDate) value), calendar);
        } else if (objClass == LocalTime.class) {
            statement.setTime(index, Time.valueOf((LocalTime) value), calendar);
        }
    }

    void setTemporal(CallableStatement statement, String parameterName, Object value, Class objClass) throws SQLException {
        if (objClass == LocalDateTime.class) {
            statement.setTimestamp(parameterName, Timestamp.valueOf((LocalDateTime) value), calendar);
        } else if (objClass == LocalDate.class) {
            statement.setDate(parameterName, java.sql.Date.valueOf((LocalDate) value), calendar);
        } else if (objClass == LocalTime.class) {
            statement.setTime(parameterName, Time.valueOf((LocalTime) value), calendar);
        }
    }

    /**
     * Obtiene un nuevo resultSet con resultados adicionales
     *
     * @param rs Provided rs
     * @return Nuevo ResultSet con resultados adicionales o null si no hay
     * resultados
     * @throws Exception Si no hay más resultados
     */
    public static ResultSet getMoreResults(ResultSet rs) throws Exception {
        Statement statement = rs.getStatement();
        return statement == null ? null : statement.getResultSet();
    }
}

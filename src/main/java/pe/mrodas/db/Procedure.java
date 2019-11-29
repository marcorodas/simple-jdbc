package pe.mrodas.db;

import java.io.IOException;
import java.io.InputStream;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import pe.mrodas.db.helper.Autoclose;
import pe.mrodas.db.helper.SqlStatement;

public class Procedure<T> extends SqlStatement<T> {

    private String name;
    private final HashMap<String, Object> parametersIn = new HashMap<>();
    private final HashMap<String, JDBCType> parametersInNull = new HashMap<>();
    private final HashMap<String, JDBCType> parametersOut = new HashMap<>();

    public Procedure(Connection connection, Autoclose autoclose) {
        super(connection, autoclose);
    }

    public Procedure() {
        super();
    }

    public void setName(String name) {
        this.name = name;
    }

    public Procedure<T> addParameterIn(String name, JDBCType type, Object value) {
        if (name == null || name.trim().isEmpty() || type == null) return this;
        parametersIn.put(name, value);
        if (value == null) parametersInNull.put(name, type);
        return this;
    }

    public Procedure<T> addParameterOut(String name, JDBCType type) {
        if (name == null || name.trim().isEmpty() || type == null) return this;
        parametersOut.put(name, type);
        return this;
    }

    public String getPreparedCall(int totalParams) throws IOException {
        if (name == null || name.trim().isEmpty()) throw new IOException("Procedure name can't be null or empty!");
        if (name.trim().split(" ").length > 1) throw new IOException("Procedure name can't have a blank space!");
        List<String> params = Collections.nCopies(totalParams, "?");
        return String.format("{CALL %s(%s)}", name.trim(), String.join(", ", params));
    }

    @Override
    protected SQLException buildCallableException(SQLException e) {
        String msj = String.format("%s Procedure:(%s)", e.getMessage(), String.format("{CALL %s(...)}", name.trim()));
        return new SQLException(msj, e);
    }

    @Override
    protected PreparedStatement executeStatement() throws IOException, SQLException {
        int totalParams = parametersIn.size() + parametersOut.size();
        String call = this.getPreparedCall(totalParams);
        CallableStatement statement = super.getConnection().prepareCall(call);
        for (Map.Entry<String, Object> parameter : parametersIn.entrySet()) {
            String name = parameter.getKey();
            Object value = parameter.getValue();
            this.tryRegisterParameter(statement, name, value, null);
        }
        for (Map.Entry<String, JDBCType> parameter : parametersOut.entrySet()) {
            String name = parameter.getKey();
            Integer sqlType = parameter.getValue().getVendorTypeNumber();
            this.tryRegisterParameter(statement, name, null, sqlType);
        }
        statement.execute();
        return statement;
    }

    private void tryRegisterParameter(CallableStatement statement, String name, Object value, Integer sqlType) throws SQLException {
        try {
            if (sqlType == null)
                if (value == null) statement.setNull(name, parametersInNull.get(name).getVendorTypeNumber());
                else this.registerParameter(statement, name, value);
            else statement.registerOutParameter(name, sqlType);
        } catch (SQLException e) {
            String typeParam = sqlType == null ? (value == null ? "NULL IN" : "IN") : "OUT";
            String error = String.format("Error setting '%s' %s parameter in statement! - ", name, typeParam);
            throw new SQLException(error + e.getMessage(), e);
        }
    }

    private void registerParameter(CallableStatement statement, String name, Object value) throws SQLException {
        Class<?> objClass = value.getClass();
        if (objClass.isArray()) {
            Class<?> componentType = value.getClass().getComponentType();
            if (componentType != null && componentType.isPrimitive())
                if (byte.class.isAssignableFrom(componentType))
                    statement.setBytes(name, (byte[]) value);
        } else if (objClass == Integer.class) statement.setInt(name, (Integer) value);
        else if (objClass == String.class) statement.setString(name, (String) value);
        else if (objClass == Boolean.class) statement.setBoolean(name, (Boolean) value);
        else if (objClass == Double.class) statement.setDouble(name, (Double) value);
        else if (objClass == Float.class) statement.setFloat(name, (Float) value);
        else if (value instanceof InputStream) statement.setBlob(name, (InputStream) value);
        else if (objClass == Date.class) {
            long time = ((Date) value).getTime();
            statement.setTimestamp(name, new Timestamp(time));
        } else if (objClass == Time.class) statement.setTime(name, (Time) value);
        else if (objClass == Timestamp.class) statement.setTimestamp(name, (Timestamp) value);
        else if (objClass == LocalDate.class)
            statement.setDate(name, java.sql.Date.valueOf((LocalDate) value));
        else if (objClass == LocalTime.class) statement.setTime(name, Time.valueOf((LocalTime) value));
        else if (objClass == LocalDateTime.class)
            statement.setTimestamp(name, Timestamp.valueOf((LocalDateTime) value));
    }

    public void execute() throws IOException, SQLException {
        try {
            this.executeStatement();
        } finally {
            super.close();
        }
    }

}

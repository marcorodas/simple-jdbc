package pe.mrodas.jdbc;

import java.io.IOException;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Enumeration;
import pe.mrodas.jdbc.DBLayer;
import org.junit.After;
import org.junit.Before;

/**
 *
 * @author skynet
 */
public abstract class TestBase {

    protected abstract String getDefaultConfigFile();

    public void config(String fileName) {
        DBLayer.Connector.configureWithPropFile(fileName);
    }

    @Before
    public void init() throws IOException {
        String fileName = this.getDefaultConfigFile();
        this.config(fileName == null ? "db.default.properties" : fileName);
    }

    @After
    public void destroy() {
        Enumeration<Driver> drivers = DriverManager.getDrivers();
        while (drivers.hasMoreElements()) {
            Driver driver = drivers.nextElement();
            try {
                DriverManager.deregisterDriver(driver);
            } catch (SQLException e) {
            }
        }
    }
}

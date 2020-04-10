package flatfilegenerator;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.dbutils.DbUtils;

public class DAOProperties {

    private static final String PROPERTIES_FILE = "dao.properties";
    private static final Properties PROPERTIES = new Properties();

    private static final String PROPERTY_URL = "db.url";
    private static final String PROPERTY_DRIVER = "db.driver";
    private static final String PROPERTY_USERNAME = "db.username";
    private static final String PROPERTY_PASSWORD = "db.password";
    private static final String PROPERTY_FILENAME = "file.name";
    private static final String PROPERTY_SQL = "file.sql";
    private static final String PROPERTY_SEPARATOR = "file.separator";

    final Logger logger = Logger.getLogger(DAOProperties.class.getName());

    private String fileName;
    private char separator;
    private String sqlQuery;

    public String getSqlQuery() {
        return sqlQuery;
    }

    public void setSqlQuery(String sqlQuery) {
        this.sqlQuery = sqlQuery;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public char getSeparator() {
        return separator;
    }

    public void setSeparator(char separator) {
        this.separator = separator;
    }

    public void intializeParam() throws FileNotFoundException, IOException {
        InputStream input = new FileInputStream(PROPERTIES_FILE);
        PROPERTIES.load(input);
        setSqlQuery(PROPERTIES.getProperty(PROPERTY_SQL));
        setFileName(PROPERTIES.getProperty(PROPERTY_FILENAME));
        setSeparator(PROPERTIES.getProperty(PROPERTY_SEPARATOR).charAt(0));
    }

    public Connection getConnection() {
        Connection conn = null;
        try {
            logger.info("Credentials getting fetched from [" + PROPERTIES_FILE + "] properties file");
            InputStream input = new FileInputStream(PROPERTIES_FILE);
            PROPERTIES.load(input);
            DbUtils.loadDriver(PROPERTIES.getProperty(PROPERTY_DRIVER));
            logger.log(Level.INFO, "DB URL: [{0}]", PROPERTIES.getProperty(PROPERTY_URL));
            logger.log(Level.INFO, "DB Username: [{0}]", PROPERTIES.getProperty(PROPERTY_USERNAME));
            logger.log(Level.INFO, "DB Password: [{0}]", PROPERTIES.getProperty(PROPERTY_PASSWORD));
            logger.log(Level.INFO, "SQL Query: [{0}]", PROPERTIES.getProperty(PROPERTY_SQL));
            logger.log(Level.INFO, "Separator: [{0}]", PROPERTIES.getProperty(PROPERTY_SEPARATOR));

            conn = DriverManager.getConnection(PROPERTIES.getProperty(PROPERTY_URL), PROPERTIES.getProperty(PROPERTY_USERNAME), PROPERTIES.getProperty(PROPERTY_PASSWORD));
        } catch (FileNotFoundException ex) {
            logger.info("Unable to find the path mentioned in the properties file");
            ex.printStackTrace();
        } catch (SQLException ex) {
            logger.info("Unable to make the connection");
            ex.printStackTrace();
        } catch (IOException ex) {
            logger.info("IO exception");
            ex.printStackTrace();
        }
        return conn;
    }
}

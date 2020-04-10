package flatfilegenerator;

import au.com.bytecode.opencsv.CSVWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class FlatFileWriter {

    static final Logger logger = Logger.getLogger(FlatFileWriter.class.getName());

    public static void main(String[] args) {
        try {
            DAOProperties dao = new DAOProperties();
            dao.intializeParam();
            Path path = Paths.get(dao.getFileName());

            try (Connection conn = dao.getConnection();
                    PreparedStatement pst = conn.prepareStatement(dao.getSqlQuery());
                    ResultSet rst = pst.executeQuery();) {

                try (CSVWriter writer = new CSVWriter(Files.newBufferedWriter(path, StandardCharsets.UTF_8),
                        dao.getSeparator(),
                        CSVWriter.NO_QUOTE_CHARACTER,
                        CSVWriter.NO_ESCAPE_CHARACTER,
                        CSVWriter.DEFAULT_LINE_END);) {
                    writer.writeAll(rst, true);
                }
            }
            logger.info("File generation complete");
        } catch (SQLException | IOException ex) {
            Logger.getLogger(FlatFileWriter.class.getName()).log(Level.SEVERE, null, ex);
            ex.printStackTrace();
        }
    }
}

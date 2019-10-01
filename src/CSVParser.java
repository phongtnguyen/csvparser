import java.io.*;
import java.sql.*;
import java.util.*;

public class CSVParser {
    private final int COL_COUNT = 10;
    private final String INSERT_SQL = "INSERT INTO Personnel(A, B, C, D, E, F, G, H, I, J) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    private final int MAX_INSERT_TRANSACTION = 100000;

    public static void main(String[] args) {
        CSVParser parser = new CSVParser();
        String path = "Data/Entry Level Coding Challenge Page 2.csv";
        parser.readFile(path);
    }

    private ArrayList<String> getAttr(String[] attr) {
        ArrayList<String> row = new ArrayList<String>();
        for (int i=0; i<attr.length; i++) {
            if (attr[i].length()>0) {
                row.add(attr[i]);
            }
        }
        return row;
    }

    private void readFile(String path) {
        Connection c = null;
        Statement stmt = null;
        PreparedStatement ps = null;

        try {
            File input = new File(path);
            BufferedReader br = new BufferedReader(new FileReader(input));

            //strip file extension
            String file_name = input.getName();
            file_name = file_name.substring(0, file_name.lastIndexOf('.'));

            String line;
            int noRecord = 0, noRecordGood = 0, noRecordBad = 0;

            //create log file
            BufferedWriter log = new BufferedWriter(new FileWriter(new File("Data/"+file_name+".log")));
            BufferedWriter bw = new BufferedWriter(new FileWriter(new File("Data/"+file_name+"-bad.csv")));

            //establish database connection
            Class.forName("org.sqlite.JDBC");
            c = DriverManager.getConnection("jdbc:sqlite:"+file_name+".db");
            c.setAutoCommit(false);
            stmt = c.createStatement();
            String query = "CREATE TABLE IF NOT EXISTS Personnel " +
                    "(ID INTEGER PRIMARY KEY, " +
                    "A  TEXT, B TEXT, C TEXT, D TEXT, E TEXT, F TEXT, G TEXT, H TEXT, I TEXT, J TEXT)";
            stmt.executeUpdate("DROP TABLE IF EXISTS Personnel");
            stmt.executeUpdate(query);
            stmt.close();
            ps = c.prepareStatement(INSERT_SQL);

            //read from file
            while ( (line=br.readLine()) != null ) {
                noRecord++;
                String[] row = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
                ArrayList<String> attr = getAttr(row);
                int cols = row.length; //number of strings including empty string
                int valid_attr = attr.size(); //number of string not including empty string
                if (cols==COL_COUNT && valid_attr==COL_COUNT) {
                    noRecordGood++;

                    ps.setString(1, attr.get(0));
                    ps.setString(2, attr.get(1));
                    ps.setString(3, attr.get(2));
                    ps.setString(4, attr.get(3));
                    ps.setString(5, attr.get(4));
                    ps.setString(6, attr.get(5));
                    ps.setString(7, attr.get(6));
                    ps.setString(8, attr.get(7));
                    ps.setString(9, attr.get(8));
                    ps.setString(10, attr.get(9));
                    ps.addBatch();

                    if ( (noRecordGood%MAX_INSERT_TRANSACTION)==0) {
                        int[] updateCounts = ps.executeBatch();
                        c.commit();
                    }
                } else {
                    noRecordBad++;
                    bw.write(line+"\n");
                }

            }
            int[] updateCounts = ps.executeBatch();
            c.commit();

            log.write("# of records received: " + noRecord+"\n");
            log.write("# of records successful: " + noRecordGood+"\n");
            log.write("# of records failed: " + noRecordBad+"\n");
            log.flush();
            log.close();
            c.close();
            br.close();
            bw.flush();
            bw.close();
        } catch (FileNotFoundException fnfe) {
            System.out.println("No such file found: "+path);
        } catch (IOException ioe) {
            System.out.println("Fail to read file: "+path);
        } catch ( Exception e ) {
            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
            System.exit(0);
        }
    }
}

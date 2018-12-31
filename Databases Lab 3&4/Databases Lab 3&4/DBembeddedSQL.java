/*
 * CSI3130 / CSI3530
 * Programming Assignment (Lab No 3 and Lab No 4)
 * 
 */

import static java.lang.Math.sqrt;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Jess Gilchrist, Henry Wu, Beatrice Johnston
 */
public class DBembeddedSQL {
    
    /**
     * Function attempts to create a connection to database, using hard-coded info.
     * @return connection to database
     */
    public static Connection getConnect(){
        Connection c = null;
        String url = "jdbc:postgresql://localhost:5432/postgres";
        String user = "postgres";
        String password = "postgres";
        try {
            Class.forName("org.postgresql.Driver");
            c = DriverManager.getConnection(url, user, password);
            c.setAutoCommit(false);
            System.out.println("Opened DB successfully");
            
        } catch (ClassNotFoundException | SQLException e) {
            System.err.println( e.getClass().getName()+": "+ e.getMessage() );
        }
        return c;
    }
    
    /**
     * Attempts to close connection C
     * @param c is the connection to be closed
     * @return true if closed properly, false otherwise.
     */
    public static boolean closeConnect(Connection c){
        try {
            c.close();
            return true;
        } catch (SQLException ex) {
            Logger.getLogger(DBembeddedSQL.class.getName()).log(Level.SEVERE, null, ex);
        }
        return false;
       }
    
    /**
     * Calculates mean of column age from table Sailors in database
     * @param c connection to database with table Sailors with column Age
     * @return the mean of Sailors.age
     */
    public static float mean (Connection c){
        try {
            Statement stmt = c.createStatement();
            ResultSet rs;
            rs = stmt.executeQuery("SELECT AVG(age) FROM Sailors;");
            float mean = 0;
            while (rs.next()){
                mean = rs.getFloat("AVG");
            }
            rs.close();
            stmt.close();
            return mean;
        } catch (SQLException e) {
            System.err.println( e.getClass().getName()+": "+ e.getMessage() );
             System.exit(0);
        }
        return 0;
    }
    

    /**
     * Calculates the variance in the ages of Sailors
     * @param c is the connection to the database with table Sailors and column Age
     * @return the variance of the ages
     */
    public static float vari (Connection c){
        float mean = mean (c);
        float ans;
        DecimalFormat df = new DecimalFormat("###############.##");
        try {
            Statement stmt = c.createStatement();
            ResultSet rs;
            rs = stmt.executeQuery("SELECT age FROM Sailors;");
            int counter = 0;
            float sumOfDiffSquared = 0;
            while (rs.next()){
                float current = rs.getFloat("age");
                current -= mean;
                current = current * current;
                current = Float.valueOf( df.format(current)); //Rounds current to 2 decimal places
                sumOfDiffSquared += current;
                counter++;
            }
            ans = sumOfDiffSquared / (float) counter;
            ans = Float.valueOf( df.format(ans)); //rounds ans to 2 decimal places
            return ans;
            
        } catch (NumberFormatException | SQLException e) {
            System.err.println( e.getClass().getName()+": "+ e.getMessage() );
            System.exit(0);
        }
        return 0;
    }
    
    /**
     * Calculates the standard deviation of the ages of Sailors
     * @param c is the connection to the database with table Sailors with column age
     * @return the standard deviation
     */
    public static double stdDev(Connection c){
        return stdDev( vari(c) );
    }
    
    /**
     * Calculates the standard deviation, given the variance
     * @param vari is the variance to calculate the standard deviation with
     * @return the standard deviation
     */
    public static double stdDev(float vari){
        return sqrt(vari);
    }
    
    /**
     * Prints all tuples within Sailors where the age is within one standard deviation of the mean age
     * @param c is the connection to the database with table Sailors with column Age
     */
    public static void ageWithinOneStdDev (Connection c){
        double mean = (double) mean(c);
        double stdDev = stdDev(c);
        double minAge = mean - stdDev;
        double maxAge = mean + stdDev;
        Statement stmt;
        ResultSet rs;
        try {
            stmt = c.createStatement();
            String query = "SELECT * FROM Sailors WHERE "
                    + "age >= " + minAge + "AND age <= " + maxAge + ";"; 
            
            rs = stmt.executeQuery(query);
            while (rs.next()){
                int sid = rs.getInt("sid");
                String sname = rs.getString("sname");
                int rating = rs.getInt("rating");
                float age = rs.getFloat("age");
                System.out.println("\nSid: " + sid);
                System.out.println("Sname: " + sname);
                System.out.println("Rating: " + rating);
                System.out.println("Age: " + age);
            }
            System.out.println("\n");
            rs.close();
            stmt.close();
        } catch (SQLException e) {
            System.err.println( e.getClass().getName()+": "+ e.getMessage() );
            System.exit(0);
        }
    }
    
        
    public static void main(String[] args) {
        Connection c =getConnect();
        float x = mean(c);
        float y = vari(c);
        double z = stdDev(y);

        System.out.println("The mean age is: " + x + " years old");
        System.out.println("The variance of the ages is: " + y);
        System.out.println("The standard deviation of the ages is: " + z);
        System.out.println("\nThe sailors with ages within one standard deviation of the mean are: ");
        ageWithinOneStdDev(c);
        closeConnect(c);
    }
}


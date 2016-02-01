/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package OracleConnection;

import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import oracle.jdbc.OracleCallableStatement;
import oracle.jdbc.internal.OracleTypes;
import oracle.sql.ARRAY;
import oracle.sql.ArrayDescriptor; 


/**
 *
 * @author Sheepman
 */
public class OracleJDBC {

    private Connection connection = null;

    public OracleJDBC() {
        try {

            Class.forName("oracle.jdbc.driver.OracleDriver");

        } catch (ClassNotFoundException e) {

            System.out.println("Where is your Oracle JDBC Driver?");
            e.printStackTrace();
            return;

        }

        System.out.println("Oracle JDBC Driver Registered!");
    }

    public Connection getConnection() {
        return this.connection;
    }

    public void openConnection() {

        try {
            this.connection = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:ORCL", "ghuser", "123456");
        } catch (SQLException e) {
            System.out.println("Connection Failed! Check output console");
            e.printStackTrace();
        }

        if (this.connection != null) {
            System.out.println("You made it, take control your database now!");
        } else {
            System.out.println("Failed to make connection!");
        }
    }

    public void closeConnection() {
        try {
            this.connection.close();
            System.out.println("Connection closed!");
        } catch (Exception e) {
            System.out.println("Connection NOT closed!");
            e.printStackTrace();
        }
    }

    public void analytics_by_hour(int[] ghArray, int[] sensorArray, String startDate, String endDate) {
        try {
            String call = "{call gh_analytics.analytics_by_hour(?,?,?,?,?)}";
            CallableStatement cstmt = this.connection.prepareCall(call);
            
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Timestamp startDateTimestamp = null;
            Timestamp endDateTimestamp = null;
            if (startDate != null){
                Date startDateParse = dateFormat.parse(startDate);
                startDateTimestamp = new Timestamp(startDateParse.getTime());
            }
            
            if (endDate != null){
                Date endDateParse = dateFormat.parse(endDate);
                endDateTimestamp = new Timestamp(endDateParse.getTime());
            }
            
            ArrayDescriptor descriptor = ArrayDescriptor.createDescriptor( "NUM_ARRAY", this.connection );
            ARRAY ghNumArray = new ARRAY( descriptor, this.connection, ghArray );
            ARRAY sensorNumArray = new ARRAY( descriptor, this.connection, sensorArray );
            cstmt.registerOutParameter(1, OracleTypes.CURSOR);
            cstmt.registerOutParameter(2, OracleTypes.ARRAY, "NUM_ARRAY");
            cstmt.setArray(2, ghNumArray);
            cstmt.registerOutParameter(3, OracleTypes.ARRAY, "NUM_ARRAY");
            cstmt.setArray(3, sensorNumArray);
            cstmt.registerOutParameter(4, OracleTypes.TIMESTAMP);
            cstmt.setTimestamp(4, startDateTimestamp);
            cstmt.registerOutParameter(5, OracleTypes.TIMESTAMP);
            cstmt.setTimestamp(5, endDateTimestamp);
            cstmt.executeQuery();
            
            
            String d1 = cstmt.getString(4);
            String d2 = cstmt.getString(5);
            
            ARRAY rset1 = (ARRAY)cstmt.getObject(2);
            BigDecimal[] values1 = (BigDecimal[])rset1.getArray();
            
            for (BigDecimal value : values1) {
                System.out.println(value);
            }
            
            ARRAY rset2 = (ARRAY)cstmt.getObject(3);
            BigDecimal[] values2 = (BigDecimal[])rset2.getArray();

            for (BigDecimal value : values2) {
                System.out.println(value);
            }        
            
            System.out.println(d1 + "   " + d2);
            
            ResultSet rset3 = (ResultSet)cstmt.getObject(1);
            while (rset3.next ()){
                System.out.println(rset3.getString ("GreenHouse") + "\t" + rset3.getString ("sensor_type") + "\t" + rset3.getInt("avg_value") + "\t");
            }
            
            rset3.close();
            cstmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }

    
}

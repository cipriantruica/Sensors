/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package greenhouses;

import OracleConnection.OracleJDBC;
/**
 *
 * @author Sheepman
 */
public class GreenHouses {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // TODO code application logic here
        // these are test to see if the analytics pachage is working
        
        int[] ghArray = new int[1];
        int[] sensorArray = new int[3];
//        ghArray = null;
//        sensorArray = null;
        ghArray[0] = (int)1002;
        sensorArray[0] = (int)1;
        sensorArray[1] = (int)3;
        sensorArray[2] = (int)6;
        
        String startDate;
        String endDate;
        
//        startDate = null;
//        endDate = null;
        startDate = "2015-09-07 13:00:00";
        
        endDate = "2015-09-21 18:00:00";
        
        OracleJDBC ojdbc = new OracleJDBC();
        
        ojdbc.openConnection();
        ojdbc.analytics_by_hour(ghArray, sensorArray, startDate, endDate);
        ojdbc.closeConnection();
    }
    
}

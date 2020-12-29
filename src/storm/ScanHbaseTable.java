/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package storm;

/**
 *
 * @author bernardo
 */


import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class ScanHbaseTable {

    static final String DB_URL = "jdbc:derby://localhost:1527/MyDatabase";

    //  Database credentials
    static final String USER = "Bernardo";
    static final String PASS = "bernardo";

    public static void main(String[] args) throws IOException {

        Configuration config = HBaseConfiguration.create(new Configuration());
        //HConnection connection = HConnectionManager.createConnection(config);
        //HTableInterface table = connection.getTable(TableName.valueOf("QueryTweetsAnalyzed"));
        //Table table= new HTable(config, TableName.valueOf("TWEETS"));
        config.set("hbase.zookeeper.property.clientPort", "2222");
        config.set("hbase.cluster.distributed", "true");
        config.set("hbase.rootdir", "hdfs://localhost:54310/hbase");
        HTable table= new HTable(config, TableName.valueOf("QueryTweetsAnalyzed2"));
        Connection conn = null;
        Statement stmt = null;
        int i =102;
        DecimalFormat decimalFormat=new DecimalFormat("00000000");
        System.out.println(decimalFormat.format(i));
        try {
            //STEP 2: Register JDBC driver
            //Class.forName("com.mysql.jdbc.Driver");

            //STEP 3: Open a connection
            System.out.println("Connecting to a selected database...");
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
            System.out.println("Connected database successfully...");

            //STEP 4: Execute a query
            System.out.println("Creating table in given database...");
            stmt = conn.createStatement();;
            String sql = "SELECT * FROM QUERYTWEETS";
            System.out.println(sql);
            ResultSet rs = stmt.executeQuery(sql);
            ArrayList<String> dataset = new ArrayList<>();
            
            while (rs.next()) {
                /*dataset.add(rs.getString("TEXT"));
                int id=Integer.parseInt(rs.getString("ID"));
                String ID = decimalFormat.format(id);
                Put p = new Put(Bytes.toBytes("row"+ID));
                p.add(Bytes.toBytes("Id"), Bytes.toBytes("AccountID"), Bytes.toBytes(rs.getString("ACCOUNTID")));
                p.add(Bytes.toBytes("Date"), Bytes.toBytes("CreationDate"), Bytes.toBytes(rs.getString("DATE")));
                p.add(Bytes.toBytes("Tweet"), Bytes.toBytes("Query"), Bytes.toBytes(rs.getString("QUERY")));
                p.add(Bytes.toBytes("Tweet"), Bytes.toBytes("Text"), Bytes.toBytes(rs.getString("TEXT")));
                p.add(Bytes.toBytes("Tweet"), Bytes.toBytes("Label"), Bytes.toBytes(rs.getString("LABEL")));
                table.put(p);*/
            }
            

            System.out.println("Insert table in given database...");

        } catch (SQLException se) {
            //Handle errors for JDBC
            se.printStackTrace();
        } catch (Exception e) {
            //Handle errors for Class.forName
            e.printStackTrace();
        } finally {
            //finally block used to close resources
            try {
                if (stmt != null) {
                    conn.close();
                }
            } catch (SQLException se) {
            }// do nothing
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException se) {
                se.printStackTrace();
            }//end finally try
        }//end finally try

  /*    
  Put p = new Put(Bytes.toBytes("row1"));

  p.add(Bytes.toBytes("Id"), Bytes.toBytes("col1"),Bytes.toBytes("TTT"));
  //p.add(Bytes.toBytes("Name"),Bytes.toBytes("col2"),Bytes.toBytes("BBB"));

  table.put(p);*/
  /*
  Put p2 = new Put(Bytes.toBytes("row4"));

  p2.add(Bytes.toBytes("Id"), Bytes.toBytes("col1"),Bytes.toBytes("AAA"));
  p2.add(Bytes.toBytes("Name"),Bytes.toBytes("col2"),Bytes.toBytes("BBB"));

  table.put(p2);
         
 /*
  Get g = new Get(Bytes.toBytes("row1"));
  Result r = table.get(g);

  byte [] value = r.getValue(Bytes.toBytes("Id"),Bytes.toBytes("col3"));
  byte [] value1 = r.getValue(Bytes.toBytes("Name"),Bytes.toBytes("col4"));

  String valueStr = Bytes.toString(value);
  String valueStr1 = Bytes.toString(value1);
  System.out.println("GET: " +"Id: "+ valueStr+" Name: "+valueStr1);
  
  Get g1 = new Get(Bytes.toBytes("row2"));
  Result r2 = table.get(g1);

  byte [] value2 = r.getValue(Bytes.toBytes("Id"),Bytes.toBytes("col3"));
  byte [] value12 = r.getValue(Bytes.toBytes("Name"),Bytes.toBytes("col4"));

  String valueStr2 = Bytes.toString(value2);
  String valueStr12 = Bytes.toString(value12);
  System.out.println("GET: " +"Id: "+ valueStr2+" Name: "+valueStr12);
         */

        Scan s = new Scan();
        s.setCaching(1);
        s.addColumn(Bytes.toBytes("Id"), Bytes.toBytes("AccountID"));
        s.addColumn(Bytes.toBytes("Tweet"), Bytes.toBytes("Text"));
        s.addColumn(Bytes.toBytes("Tweet"), Bytes.toBytes("Label"));
        ResultScanner scanner = table.getScanner(s);
        try {
            for (Result result = scanner.next(); result != null; result = scanner.next()) {
                //System.out.println("Found Row:" + result);

                byte[] value = result.getValue(Bytes.toBytes("Id"), Bytes.toBytes("AccountID"));
                byte[] value1 = result.getValue(Bytes.toBytes("Tweet"), Bytes.toBytes("Text"));
                byte[] value2 = result.getValue(Bytes.toBytes("Tweet"), Bytes.toBytes("Label"));

                String valueStr = Bytes.toString(value);
                String valueStr1 = Bytes.toString(value1);
                String valueStr2 = Bytes.toString(value2);
                String row=Bytes.toString(result.getRow());
                System.out.println("Row "+row + " Id: " + valueStr + " Text: " + valueStr1+ " Label: " + valueStr2);
            }
            scanner.close();
            //}
        } catch (Exception ex) {

        }

    }
}


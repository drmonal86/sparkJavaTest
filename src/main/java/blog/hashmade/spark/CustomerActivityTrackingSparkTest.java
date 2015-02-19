package blog.hashmade.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SchemaRDD;
import org.apache.spark.sql.cassandra.CassandraSQLContext;
import org.apache.spark.sql.catalyst.expressions.Row;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;




import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;

import javax.xml.bind.DatatypeConverter;

import java.io.IOException;

/**
 */
public class CustomerActivityTrackingSparkTest {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(CustomerActivityTrackingSparkTest.class);
  public static final String DESTINATION_TEXT_FIELD = "destination_text";

  public static void main(String[] args) throws IOException {
    try {
      
      initSpark(args[0]);

    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      System.exit(1);
    }
  }

  public static void initSpark(String id) throws Exception {
    SparkConf conf = new SparkConf(true)
        .setMaster("local")
        .setAppName("DatastaxtTests")
        .set("spark.executor.memory", "1g")
        .set("spark.cassandra.connection.host", "172.28.65.97")
        .set("spark.cassandra.connection.native.port", "9042");
    SparkContext javaSparkContext = new SparkContext(conf);

   CassandraSQLContext csqlctx = new CassandraSQLContext(javaSparkContext);
    csqlctx.setKeyspace("mdoctor");
    SchemaRDD
        schemaRDD = csqlctx.sql("SELECT * FROM customer_activity_tracking WHERE id =" +id);
    
    Row[] rowschemaRDD= schemaRDD.collect();
    
   System.out.println("Number of rows Schema returned " + rowschemaRDD.length);
   schemaRDD.registerTempTable("activity");

   SchemaRDD filterRDD = csqlctx.sql("Select * FROM customer_activity_tracking_filters where id = "+ id);
    Row[] rowfilterRDD= filterRDD.collect();
    System.out.println("Number of rows FILTER returned " + rowfilterRDD);
    
   filterRDD.registerTempTable("filter");
   
   
   SchemaRDD joined = csqlctx.sql("Select a.browser_type_desc,a.activity_details,f.amenity_filter_selection_desc, f.brand_filter_selection_desc FROM filter f JOIN activity a ON f.web_session_id = a.web_session_id");     

   joined.cache();
    Row[] rows = joined.collect();
    System.out.println("Number of rows JOINED returned " + rows.length);
 
    
   
    int i = 0;
   String activityDetails = "";
    if (rows.length > 0) {
     LOGGER.info("ROW\tID\t\tHIGHEST PRICE\t\tBRAND PREFERENCE\tDESTINATION TEXT\t\t\t");
     for (Row row : rows) {
        i++;
        // Pull destination text from activity details blob

        String activityTypeHexString = row.toString();
        System.out.println(activityTypeHexString);
       // System.out.println(activityTypeHexString);
        // Trim 0x from beginning of hex string
        //String activityTypeASCIIString = convertHexToString(activityTypeHexString.substring(2));
      // JSONObject jsonObject = new JSONObject(activityTypeASCIIString);

       // LOGGER.info( i + "\t\t" + row.getString(0) + "\t" + row.getString(1) + "\t" + row.getString(2) + "\t\t" + jsonObject.get("destinationText"));
    //   LOGGER.info( i + "\t\t" + ((Row) row).getString(0) + "\t" + row.getInt(1));
     }
    }

  }
  public static String convertHexToString(String hex){

    StringBuilder sb = new StringBuilder();
    StringBuilder temp = new StringBuilder();

    //49204c6f7665204a617661 split into two characters 49, 20, 4c...
    for( int i=0; i<hex.length()-1; i+=2 ){

      //grab the hex in pairs
      String output = hex.substring(i, (i + 2));
      //convert hex to decimal
      int decimal = Integer.parseInt(output, 16);
      //convert the decimal to character
      sb.append((char)decimal);

      temp.append(decimal);
    }

    return sb.toString();
  }


}

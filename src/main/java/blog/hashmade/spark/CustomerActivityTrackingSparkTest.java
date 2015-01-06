package blog.hashmade.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SchemaRDD;
import org.apache.spark.sql.cassandra.CassandraSQLContext;
import org.apache.spark.sql.catalyst.expressions.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

      initSpark();
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      System.exit(1);
    }
  }



	/*public static void initSpark() {
		SparkConf conf = new SparkConf(true)
				.setMaster("local")
	            .setAppName("DatastaxtTests")
	            .set("spark.executor.memory", "1g")
				.set("spark.cassandra.connection.host", "localhost")
				.set("spark.cassandra.connection.native.port", "9142")
				.set("spark.cassandra.connection.rpc.port", "9171");
		SparkContext ctx = new SparkContext(conf);
		SparkContextJavaFunctions functions = CassandraJavaUtil.javaFunctions(ctx);
		CassandraJavaRDD<CassandraRow> rdd = functions.cassandraTable("roadtrips", "roadtrip");
		rdd.cache();*/


  public static void initSpark() {
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
        schemaRDD = csqlctx.sql("SELECT id, timestamp, activity_type_desc, activity_details FROM customer_activity_tracking WHERE id = '50272629TE32268684'");
    schemaRDD.cache();

    Row[] rows = schemaRDD.collect();
    System.out.println("Number of rows returned " + rows.length);
    int i = 0;
    String activityDetails = "";
    if (rows.length > 0) {
      LOGGER.info("ROW\t\tID\t\t\t\tTIMESTAMP\t\tACTIVITY TYPE DESC\t\t\tDESTINATION TEXT");
      for (Row row : rows) {
        i++;
        // Pull destination text from activity details blob
        String activityTypeHexString = row.getString(3);
        // Trim 0x from beginning of hex string
        String activityTypeASCIIString = convertHexToString(activityTypeHexString.substring(2));
        LOGGER.info( i + "\t\t" + row.getString(0) + "\t" + row.getString(1) + "\t" + row.getString(2));
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
    System.out.println("Decimal : " + temp.toString());

    return sb.toString();
  }


}

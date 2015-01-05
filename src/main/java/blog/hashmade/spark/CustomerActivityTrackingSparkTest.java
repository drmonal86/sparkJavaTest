package blog.hashmade.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.catalyst.expressions.Row;
import org.apache.spark.sql.SchemaRDD;
import org.apache.spark.sql.cassandra.CassandraSQLContext;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thinkaurelius.thrift.TDisruptorServer.Args;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

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

  public static void initSpark(String id) {
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

        schemaRDD = csqlctx.sql("SELECT id, timestamp, activity_type_desc, activity_details FROM customer_activity_tracking WHERE id="+id);

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

        LOGGER.info( i + "\t\t" + row.getString(0) + "\t" + row.getString(1) + "\t" + row.getString(2));
      }
    }

  }


}

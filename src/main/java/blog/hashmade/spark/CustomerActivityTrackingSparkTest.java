package blog.hashmade.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.catalyst.expressions.Row;
import org.apache.spark.sql.SchemaRDD;
import org.apache.spark.sql.cassandra.CassandraSQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 */
public class CustomerActivityTrackingSparkTest {
  private static final Logger LOGGER = LoggerFactory
      .getLogger(CustomerActivityTrackingSparkTest.class);

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
        schemaRDD = csqlctx.sql("SELECT * FROM customer_activity_tracking WHERE id = '50272629TE32268684'");
    schemaRDD.cache();

    Row[] rows = schemaRDD.collect();
    System.out.println("Number of rows returned " + rows.length);
  }

}

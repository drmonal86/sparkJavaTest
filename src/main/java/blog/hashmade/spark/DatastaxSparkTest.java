package blog.hashmade.spark;

import java.io.IOException;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.SparkContextJavaFunctions;
import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;
import com.google.common.collect.Lists;

public class DatastaxSparkTest {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(DatastaxSparkTest.class);

	public static void main(String[] args) throws IOException {
		try {
			blog.hashmade.spark.util.RoadTripUtil.initCassandraWithRoadTrips();
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
    SparkContext ctx = new SparkContext(conf);
    SparkContextJavaFunctions functions = CassandraJavaUtil.javaFunctions(ctx);
    CassandraJavaRDD<CassandraRow> rdd = functions.cassandraTable("mdoctor", "roadtrip");
    rdd.cache();
				
		JavaPairRDD<String, Integer> sizes = rdd.groupBy( new Function<CassandraRow, String>() {
			@Override
			public String call(CassandraRow row) throws Exception {
				return row.getString("origin_city_name");
			}
		}).
		mapToPair(new PairFunction<Tuple2<String,Iterable<CassandraRow>>, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(Tuple2<String, Iterable<CassandraRow>> t) throws Exception {
				return new Tuple2<String,Integer>(t._1(), Lists.newArrayList(t._2()).size());
			}
		});
		sizes.cache();
	
		List<Tuple2<String, Integer>> sizesResults = sizes.collect();
		LOGGER.info("Nb RoadTrips by origin");
		for(Tuple2<String, Integer> tuple : sizesResults){
			LOGGER.info(tuple._1() + " : " + tuple._2());
		}
		
		JavaPairRDD<String, Integer> sums = rdd.mapToPair(new PairFunction<CassandraRow, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(CassandraRow row)
					throws Exception {
				return new Tuple2(row.getString("origin_city_name"), row.getInt("distance"));
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer d1, Integer d2) throws Exception {
				return Integer.valueOf(d1.intValue()+d2.intValue());
			}
		});
		List<Tuple2<String,Double>> averageResults = sums.join(sizes).mapValues(new Function<Tuple2<Integer,Integer>, Double>() {
			@Override
			public Double call(Tuple2<Integer, Integer> tuple) throws Exception {
				return Double.valueOf((double)tuple._1() / tuple._2());
			}
		}).collect();
		
		LOGGER.info("Average distance by origin");
		for(Tuple2<String, Double> tuple : averageResults){
			LOGGER.info(tuple._1() + " : " + tuple._2());
		}
		
	}
}

package blog.hashmade.spark.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;
import java.util.zip.ZipFile;

import org.cassandraunit.CQLDataLoader;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.supercsv.cellprocessor.ParseInt;
import org.supercsv.cellprocessor.constraint.NotNull;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvBeanReader;
import org.supercsv.io.ICsvBeanReader;
import org.supercsv.prefs.CsvPreference;

import blog.hashmade.spark.csv.RoadTrip;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public final class RoadTripUtil {

	public static final int INSERT_SIZE = 1000;
	public static final Logger LOGGER = LoggerFactory.getLogger(RoadTripUtil.class);
	
	private RoadTripUtil(){
	}

	public static void initCassandraWithRoadTrips() {
		Session session = null;
		ICsvBeanReader beanReader = null;
		InputStream inputStream = null;
		ZipFile zipFile = null;
		try {
			session = CassandraUtil.startCassandra();
			CQLDataLoader dataLoader = new CQLDataLoader(session);
			dataLoader.load(new ClassPathCQLDataSet("roadtrips.cql"));
			
			Insert insertStatement = QueryBuilder.insertInto("mdoctor","roadtrip");
			insertStatement.value("id", QueryBuilder.bindMarker())
					.value("origin_city_name", QueryBuilder.bindMarker())
					.value("origin_state_abr", QueryBuilder.bindMarker())
					.value("destination_city_name", QueryBuilder.bindMarker())
					.value("destination_state_abr", QueryBuilder.bindMarker())
					.value("elapsed_time", QueryBuilder.bindMarker())
					.value("distance", QueryBuilder.bindMarker())
			;
			PreparedStatement ps = session.prepare(insertStatement.toString());
			
			zipFile = new ZipFile("src/main/resources/roadtrips.zip");
			inputStream = zipFile.getInputStream(zipFile.entries().nextElement());
	
			beanReader = new CsvBeanReader(new BufferedReader(new InputStreamReader(inputStream, "UTF-8")),	CsvPreference.STANDARD_PREFERENCE);
	
			final String[] header = beanReader.getHeader(true);
			CellProcessor[] processors = new CellProcessor[]{
				new ParseInt(),//id
				new NotNull(),//origin_city_name
				new NotNull(),//origin_state_abr
				new NotNull(),//destination_city_name
				new NotNull(),//destination_state_abr
				new ParseInt(),//elapsed_time
				new ParseInt(),//distance
			};
			List<RoadTrip> roadtrips = new LinkedList<RoadTrip>();
			RoadTrip roadtrip;
			while ((roadtrip = beanReader.read(RoadTrip.class, header, processors)) != null) {
				roadtrips.add(roadtrip);
				if(roadtrips.size()%RoadTripUtil.INSERT_SIZE == 0){
					RoadTripUtil.populateCassandraWithRoadTrips(session, ps, roadtrips);
					roadtrips.clear();
				}
			}
			/*if(roadtrips.size()>0){
				populateCassandraWithRoadTrips(session, ps, roadtrips);
				roadtrips.clear();
			}*/
	
		} catch (Exception e) {
			RoadTripUtil.LOGGER.error(e.getMessage(), e);
		} finally {
			try {
				if (beanReader != null) {
					beanReader.close();
				}
				if (zipFile != null) {
					zipFile.close();
				}
				if (session != null) {
					session.close();
				}
			} catch (IOException e) {
				RoadTripUtil.LOGGER.error(e.getMessage(), e);
			}
		}
	
	}

	private static void populateCassandraWithRoadTrips(Session session, PreparedStatement ps, List<RoadTrip> roadtrips){
		BatchStatement batch = new BatchStatement();
		for (RoadTrip roadtrip : roadtrips) {
			batch.add(ps.bind(roadtrip.getId(),
					roadtrip.getOriginCityName(),
					roadtrip.getOriginStateAbr(),
					roadtrip.getDestinationCityName(),
					roadtrip.getDestinationStateAbr(),
					roadtrip.getElapsedTime(),
					roadtrip.getDistance()
					));
		}
		session.execute(batch);
	}
}

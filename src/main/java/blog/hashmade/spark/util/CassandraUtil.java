package blog.hashmade.spark.util;

import org.cassandraunit.utils.EmbeddedCassandraServerHelper;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.Session;

public final class CassandraUtil {

	public static final int EMBEDDED_CASSANDRA_SERVER_WAITING_TIME = 10000;

	private CassandraUtil(){
	}

	static Session startCassandra() throws Exception {
		EmbeddedCassandraServerHelper.startEmbeddedCassandra();
		Thread.sleep(EMBEDDED_CASSANDRA_SERVER_WAITING_TIME);
		Cluster cluster = new Cluster.Builder().addContactPoints("172.28.65.97")
				.withPort(9042).build();
		Session session = cluster.connect();
		return session;
	}
}

package blog.hashmade.spark.util;

import java.util.List;

import org.cassandraunit.CQLDataLoader;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import blog.hashmade.spark.retrofit.bean.Match;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public final class MatchUtil {

	public static final Logger LOGGER = LoggerFactory.getLogger(MatchUtil.class);

	private MatchUtil() {
	}

	public static void initCassandraWithMatchs(List<Match> matchs)
			throws Exception {
		Session session = CassandraUtil.startCassandra();
		CQLDataLoader dataLoader = new CQLDataLoader(session);
		dataLoader.load(new ClassPathCQLDataSet("worldcup.cql"));

		Insert insertStatement = QueryBuilder.insertInto("Match");
		insertStatement.value("number", QueryBuilder.bindMarker())
				.value("status", QueryBuilder.bindMarker())
				.value("location", QueryBuilder.bindMarker())
				.value("datetime", QueryBuilder.bindMarker())
				.value("winner", QueryBuilder.bindMarker())
				.value("home_team_code", QueryBuilder.bindMarker())
				.value("home_team_country", QueryBuilder.bindMarker())
				.value("home_team_goals", QueryBuilder.bindMarker())
				.value("away_team_code", QueryBuilder.bindMarker())
				.value("away_team_country", QueryBuilder.bindMarker())
				.value("away_team_goals", QueryBuilder.bindMarker());
		PreparedStatement ps = session.prepare(insertStatement.toString());
		BatchStatement batch = new BatchStatement();
		for (Match match : matchs) {
			batch.add(ps.bind(match.getMatch_number(), match.getStatus(), match
					.getLocation(), match.getDatetime(), match.getWinner(),
					match.getHome_team().getCode(), match.getHome_team()
							.getCountry(), match.getHome_team().getGoals(),
					match.getAway_team().getCode(), match.getAway_team()
							.getCountry(), match.getAway_team().getGoals()));
		}
		session.execute(batch);
		session.close();

	}
}

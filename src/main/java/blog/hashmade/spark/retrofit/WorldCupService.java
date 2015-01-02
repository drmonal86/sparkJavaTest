package blog.hashmade.spark.retrofit;

import java.util.List;

import retrofit.http.GET;
import blog.hashmade.spark.retrofit.bean.Match;

public interface WorldCupService {

	@GET("/matches")
	List<Match> getMatchs();
}

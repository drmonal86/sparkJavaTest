package blog.hashmade.spark.retrofit.bean;


public class Match {

	private int match_number=-1;
	private String location=null;
	private String status=null;
	private String datetime=null;
	private String winner=null;
	
	private Team home_team = null;
	private Team away_team = null;
	
	public int getMatch_number() {
		return match_number;
	}
	public void setMatch_number(int match_number) {
		this.match_number = match_number;
	}
	public String getLocation() {
		return location;
	}
	public void setLocation(String location) {
		this.location = location;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public String getDatetime() {
		return datetime;
	}
	public void setDatetime(String datetime) {
		this.datetime = datetime;
	}
	public String getWinner() {
		return winner;
	}
	public void setWinner(String winner) {
		this.winner = winner;
	}
	public Team getHome_team() {
		return home_team;
	}
	public void setHome_team(Team home_team) {
		this.home_team = home_team;
	}
	public Team getAway_team() {
		return away_team;
	}
	public void setAway_team(Team away_team) {
		this.away_team = away_team;
	}
	
}

package blog.hashmade.spark.csv;


public class RoadTrip {
	
	private Integer id = null;
	private String originCityName = null;
	private String originStateAbr = null;
	private String destinationCityName = null;
	private String destinationStateAbr = null;
	private Integer elapsedTime = null;
	private Integer distance = null;
	
	public RoadTrip(){
	}

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public String getOriginCityName() {
		return originCityName;
	}

	public void setOriginCityName(String originCityName) {
		this.originCityName = originCityName;
	}

	public String getOriginStateAbr() {
		return originStateAbr;
	}

	public void setOriginStateAbr(String originStateAbr) {
		this.originStateAbr = originStateAbr;
	}

	public String getDestinationCityName() {
		return destinationCityName;
	}

	public void setDestinationCityName(String destinationCityName) {
		this.destinationCityName = destinationCityName;
	}

	public String getDestinationStateAbr() {
		return destinationStateAbr;
	}

	public void setDestinationStateAbr(String destinationStateAbr) {
		this.destinationStateAbr = destinationStateAbr;
	}

	public Integer getElapsedTime() {
		return elapsedTime;
	}

	public void setElapsedTime(Integer elapsedTime) {
		this.elapsedTime = elapsedTime;
	}

	public Integer getDistance() {
		return distance;
	}

	public void setDistance(Integer distance) {
		this.distance = distance;
	}

	

	
}

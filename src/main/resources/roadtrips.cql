CREATE KEYSPACE RoadTrips WITH replication={'class' : 'SimpleStrategy', 'replication_factor':1};

use RoadTrips;

CREATE TABLE RoadTrip (
	id int PRIMARY KEY,
	origin_city_name varchar,
	origin_state_abr varchar,
	destination_city_name varchar,
	destination_state_abr varchar,
	elapsed_time int,
	distance int
);
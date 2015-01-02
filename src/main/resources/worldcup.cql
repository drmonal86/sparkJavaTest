CREATE KEYSPACE WorldCup WITH replication={'class' : 'SimpleStrategy', 'replication_factor':1};

use WorldCup;

CREATE TABLE Match (

	number int PRIMARY KEY,
	status varchar,
	location varchar,
	datetime  varchar,
	winner varchar,
	home_team_code varchar,
	home_team_country varchar,
	home_team_goals int,
	away_team_code varchar,
	away_team_country varchar,
	away_team_goals int
	
);
/* 
Divvy Rider Info Exploration and Data-Cleaning

Skills used: Converting Data Types, Joins, Aggregate Functions, Subqueries, Creating Views, Unions, Case Statements

*/

/* importing csv files
 all 4 quarters (q1 2020 without station names and map coordinates) */
 
CREATE TABLE divvy_2019_q4_trips_users (
	ride_id VARCHAR(50),
	start_date DATE,
	start_time TIME,
	end_date DATE,
	end_time TIME,
	start_station_id INT,
	end_station_id INT,
	user_type VARCHAR(20),
	day_of_week INT
);


-- table for q1 2020 locations and coordinates

CREATE TABLE divvy_2020_q1_trips_locations (
	ride_id VARCHAR(50),
	start_station VARCHAR(75),
	start_station_id INT,
	start_lat NUMERIC(6,4),
	start_lng NUMERIC(6,4),
	end_station VARCHAR(75),
	end_station_id INT,
	end_lat NUMERIC(6,4),
	end_lng NUMERIC(6,4)
);


/* view for all stations and their map coordinates
 to be joined to other tables via station_id */
CREATE VIEW station_coordinates AS
	SELECT start_station AS station, start_station_id AS station_id, start_lat AS latitude, start_lng AS longitude 
	FROM divvy_2020_q1_trips_locations
	GROUP BY start_station, start_station_id, start_lat, start_lng;


/* table for demographic breakdown of subscribers q2 2019 - q4 2019
 birth year needs to be imported as a string */
 
CREATE TABLE subscriber_demographics_q4_2019 (
	ride_id VARCHAR(50),
	gender VARCHAR(10),
	birth_year VARCHAR(10)
);


/* convert birth_year into date data-type 
(for simplicity, all dated January 1st) */

ALTER TABLE subscriber_demographics_q4_2019
ALTER COLUMN birth_year type DATE using to_date(birth_year, 'yyyy-mm-dd');


--delete null fields

DELETE FROM subscriber_demographics_q4_2019
WHERE gender IS NULL OR birth_year IS NULL;


/* view with concatenated and converted timestamps, 
 ride_length, and verbal day of week
 run for all 4 quarters */
 
CREATE TABLE divvy_q1_2020_rides AS
SELECT *, end_time::TIMESTAMP - start_time::TIMESTAMP AS ride_length
FROM
(SELECT ride_id, start_date || ' ' || start_time AS start_time, 
	end_date || ' ' || end_time AS end_time, start_station_id, end_station_id, 
 	CASE 
 		WHEN user_type = 'Subscriber' THEN 'Member'
 		WHEN user_type = 'Customer' THEN 'Casual' 
 	END AS user_type,
	CASE 
		WHEN day_of_week = 1 THEN 'Sunday'
		WHEN day_of_week = 2 THEN 'Monday'
		WHEN day_of_week = 3 THEN 'Tuesday'
		WHEN day_of_week = 4 THEN 'Wednesday'
		WHEN day_of_week = 5 THEN 'Thursday'
		WHEN day_of_week = 6 THEN 'Friday'
		WHEN day_of_week = 7 THEN 'Saturday'
	END AS day_of_week
FROM divvy_2020_q1_trips_users) a;

-- new column for just time and update, setting equal to start_time
ALTER TABLE divvy_q4_2019_rides
ADD COLUMN time time;

UPDATE divvy_q4_2019_rides
SET time = start_time::time


-- extract hour from new time column to create more consistent time categories and filter by weekdays
CREATE TABLE divvy_q4_2019_rides1 AS
	SELECT ride_id, start_station_id, user_type, day_of_week, ride_length, extract(hour FROM time) AS hour
	FROM divvy_q4_2019_rides
	WHERE day_of_week IN('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday');


-- create table of weekday ride times for Casual and Members for viz
CREATE VIEW weekday_rides_by_time AS
SELECT user_type, SUM(count),
	CASE 
	WHEN hour IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11) THEN CAST(hour as varchar(5)) || ' AM'
	WHEN hour = 12 THEN '12 PM'
	WHEN hour IN(13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23) THEN CAST(hour - 12 as varchar(5)) || ' PM'
	WHEN hour = 0 THEN '12 AM'
	END as time
	FROM
		(SELECT user_type, hour, COUNT(*) FROM divvy_q1_2020_rides1
		GROUP BY user_type, hour 
			UNION ALL 
		SELECT user_type, hour, COUNT(*) FROM divvy_q2_2019_rides1
		GROUP BY user_type, hour
			UNION ALL 
		SELECT user_type, hour, COUNT(*) FROM divvy_q3_2019_rides1
		GROUP BY user_type, hour
			UNION ALL 
		SELECT user_type, hour, COUNT(*) FROM divvy_q4_2019_rides1
		GROUP BY user_type, hour) a
	GROUP BY user_type, hour;


/* find out what demographic has the most subscriptions
 run q2 2019 - q4 2019 */
 
CREATE VIEW member_demographics AS
	SELECT *, 
		CASE
		WHEN age < 18 THEN '-18'
		WHEN age >= 18 AND age <= 25 THEN '18-25'
		WHEN age >= 26 AND age <= 40 THEN '26-40'
		WHEN age >= 41 AND age <= 64 THEN '41-64'
		WHEN age >= 65 THEN '65+'
		END AS age_group
	FROM (
		SELECT gender, 
		DATE_PART('year', '2019-01-01'::DATE) - DATE_PART('year', birth_year) AS age
		FROM subscriber_demographics_q2_2019
		WHERE gender IS NOT NULL 
			UNION ALL
		SELECT gender, 
		DATE_PART('year', '2019-01-01'::DATE) - DATE_PART('year', birth_year) AS age
		FROM subscriber_demographics_q3_2019
		WHERE gender IS NOT NULL 
			UNION ALL
		SELECT gender,
		DATE_PART('year', '2019-01-01'::DATE) - DATE_PART('year', birth_year) AS age
		FROM subscriber_demographics_q4_2019
		WHERE gender IS NOT NULL) a
	WHERE age BETWEEN 10 AND 90
	ORDER BY age DESC;

	
	--partition by age_group and gender, aggregate, and export for viz
SELECT gender, age_group, COUNT(*) 
FROM member_demographics
GROUP BY gender, age_group
ORDER BY age_group, gender;


/* View with ride_length differences between casual and member riders
 4 quarters compiled */
	
CREATE VIEW ride_length_by_day AS
	SELECT day_of_week, ride_length, user_type, SUM(count)
	FROM
	(SELECT day_of_week, ride_length, user_type, COUNT(*)
	FROM divvy_q1_2020_rides
	GROUP BY day_of_week, ride_length, user_type
		UNION ALL
	SELECT day_of_week, ride_length, user_type, COUNT(*)
	FROM divvy_q2_2019_rides
	GROUP BY day_of_week, ride_length, user_type
		UNION ALL
	SELECT day_of_week, ride_length, user_type, COUNT(*)
	FROM divvy_q3_2019_rides
	GROUP BY day_of_week, ride_length, user_type
		UNION ALL
	SELECT day_of_week, ride_length, user_type, COUNT(*)
	FROM divvy_q4_2019_rides
	GROUP BY day_of_week, ride_length, user_type) a
	WHERE ride_length > '00:00:00'
	GROUP BY day_of_week, ride_length, user_type;


	--create ride duration ranges for viz
SELECT day_of_week, user_type, duration_group, SUM(sum)
FROM (
	SELECT *,
		CASE 
		WHEN ride_length < '00:30:00' THEN '<30 Mins'
		WHEN ride_length >= '00:30:00' AND ride_length <= '01:00:00' THEN '30 mins - 1 Hr'
		WHEN ride_length >= '01:00:01' AND ride_length <= '03:00:00' THEN '1-3 Hrs'
		WHEN ride_length > '03:00:00' THEN '3+ Hrs'
		END AS duration_group
	FROM ride_length_by_day) a
GROUP BY day_of_week, duration_group, user_type;


/* View with city locations of casual and member riders
 4 quarters compiled 
 export for viz */
 
CREATE VIEW station_uses AS
	SELECT station, latitude, longitude, user_type, SUM(station_count) AS station_count 
	FROM
		(SELECT station, latitude, longitude, user_type, COUNT(*) AS station_count
		FROM divvy_q1_2020_rides d
		JOIN station_coordinates s ON d.start_station_id = s.station_id
		GROUP BY station, latitude, longitude, user_type
			UNION ALL
		SELECT station, latitude, longitude, user_type, COUNT(*) AS station_count
		FROM divvy_q2_2019_rides d
		JOIN station_coordinates s ON d.start_station_id = s.station_id
		GROUP BY station, latitude, longitude, user_type
			UNION ALL
		SELECT station, latitude, longitude, user_type, COUNT(*) AS station_count
		FROM divvy_q3_2019_rides d
		JOIN station_coordinates s ON d.start_station_id = s.station_id
		GROUP BY station, latitude, longitude, user_type
			UNION ALL
		SELECT station, latitude, longitude, user_type, COUNT(*) AS station_count
		FROM divvy_q4_2019_rides d
		JOIN station_coordinates s ON d.start_station_id = s.station_id
		GROUP BY station, latitude, longitude, user_type) a
	GROUP BY station, latitude, longitude, user_type;


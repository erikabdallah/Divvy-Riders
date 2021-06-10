/* 
Divvy Rider Info Exploration

Skills used: Converting Data Types, Joins, Aggregate Functions, Subqueries, 
Creating Views, Unions, Case Statements

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
create view station_coordinates AS
select start_station as station, start_station_id AS station_id, start_lat as latitude, start_lng as longitude from divvy_2020_q1_trips_locations
group by start_station, start_station_id, start_lat, start_lng
union
select end_station as station, end_station_id AS station_id, end_lat as latitude, end_lng as longitude from divvy_2020_q1_trips_locations
group by end_station, end_station_id, end_lat, end_lng
order by station_id;


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
 
CREATE VIEW divvy_q1_2020_rides AS
SELECT *, end_time::TIMESTAMP - start_time::TIMESTAMP AS ride_length
FROM
(select ride_id, start_date || ' ' || start_time AS start_time, 
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
from divvy_2020_q1_trips_users) a;


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
order by age desc
;

	
	--partition by age_group and gender, aggregate, and export for viz
select gender, age_group, COUNT(*) 
from member_demographics
GROUP BY gender, age_group
order by age_group, gender;


/* 2 views - 1 for casual users and 1 for members 
 ride_length differences between casual and member riders
 4 quarters compiled */
	
CREATE VIEW ride_length_by_day AS
SELECT day_of_week, ride_length, user_type, sum(count)
FROM
(SELECT day_of_week, ride_length, user_type, count(*)
FROM divvy_q1_2020_rides
GROUP BY day_of_week, ride_length, user_type
	UNION ALL
SELECT day_of_week, ride_length, user_type, count(*)
FROM divvy_q2_2019_rides
GROUP BY day_of_week, ride_length, user_type
	UNION ALL
SELECT day_of_week, ride_length, user_type, count(*)
FROM divvy_q3_2019_rides
GROUP BY day_of_week, ride_length, user_type
	UNION ALL
SELECT day_of_week, ride_length, user_type, count(*)
FROM divvy_q4_2019_rides
GROUP BY day_of_week, ride_length, user_type) a
WHERE ride_length > '00:00:00'
GROUP BY day_of_week, ride_length, user_type;


	--create ride duration ranges from each view for viz
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


/* 2 views - 1 for members and 1 for casual users 
 with city locations of casual and member riders
 4 quarters compiled 
 export for viz */
 
CREATE VIEW station_uses AS
SELECT station, latitude, longitude, user_type, SUM(station_count) AS station_count 
FROM
	(SELECT station, latitude, longitude, user_type, COUNT(ride_id) AS station_count
	FROM divvy_q1_2020_rides d
	JOIN station_coordinates s ON d.start_station_id = s.station_id
	GROUP BY station, latitude, longitude, user_type
		UNION ALL
	SELECT station, latitude, longitude, user_type, COUNT(ride_id) AS station_count
	FROM divvy_q2_2019_rides d
	JOIN station_coordinates s ON d.start_station_id = s.station_id
	GROUP BY station, latitude, longitude, user_type
		UNION ALL
	SELECT station, latitude, longitude, user_type, COUNT(ride_id) AS station_count
	FROM divvy_q3_2019_rides d
	JOIN station_coordinates s ON d.start_station_id = s.station_id
	GROUP BY station, latitude, longitude, user_type
		UNION ALL
	SELECT station, latitude, longitude, user_type, COUNT(ride_id) AS station_count
	FROM divvy_q4_2019_rides d
	JOIN station_coordinates s ON d.start_station_id = s.station_id
	GROUP BY station, latitude, longitude, user_type) a
GROUP BY station, latitude, longitude, user_type;

	/* find out times of casual users rides during the week and export for viz
	 to find out if casual users and members have similar riding patterns  */
	 
SELECT ride_id, start_time, day_of_week 
FROM ride_length_member
WHERE day_of_week IN('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday')
AND ride_length < '00:30:00'
ORDER BY start_time;



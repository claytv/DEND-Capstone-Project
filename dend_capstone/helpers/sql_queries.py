#### Drops

DROP_STAGING_WEATHER = """DROP TABLE IF EXISTS public.staging_weather"""
DROP_STAGING_AIRPORT_CODES = """DROP TABLE IF EXISTS public.staging_airport_codes"""
DROP_STAGING_US_DEMOGRAPHICS = """DROP TABLE IF EXISTS public.staging_us_demographics"""
DROP_STAGING_FLIGHTS = """DROP TABLE IF EXISTS public.staging_flights"""
DROP_FLIGHTS = """DROP TABLE IF EXISTS public.flights"""
DROP_CITIES = """DROP TABLE IF EXISTS public.cities"""
DROP_WEATHER =  """DROP TABLE IF EXISTS public.weather"""
DROP_DEMOGRAPHICS = """DROP TABLE IF EXISTS public.demographics"""
DROP_TIME = """DROP TABLE IF EXISTS public.time"""
#### Creates

CREATE_STAGING_WEATHER = """
    CREATE TABLE public.staging_weather (
        dt                          DATE, 
        average_temp                DECIMAL(19,3),
        average_temp_uncertainty    DECIMAL(19,3),
        city                        VARCHAR,
        country                     VARCHAR, 
        latitude                    VARCHAR, 
        longitude                   VARCHAR
        );
"""


CREATE_STAGING_US_DEMOGRAPHICS = """ 
    CREATE TABLE public.staging_us_demographics (
        city                        VARCHAR,
        state                       VARCHAR,
        median_age                  FLOAT,
        male_population             BIGINT,
        female_population           BIGINT,
        total_population            BIGINT,
        number_of_veterans          BIGINT,
        foreign_born                BIGINT,
        average_household_size      DECIMAL(6,3),
        state_code                  VARCHAR,
        race                        VARCHAR,
        count                       BIGINT
    );
"""

CREATE_STAGING_FLIGHTS = """
    CREATE TABLE public.staging_flights (
        year                        INTEGER,
        month                       INTEGER,
        day_of_month                INTEGER,
        date                        DATE, 
        origin_city_name            VARCHAR, 
        origin_state_abr            VARCHAR,
        dest_city_name              VARCHAR,
        dest_state_abr              VARCHAR
    );

"""


CREATE_FLIGHTS = """
    CREATE TABLE public.flights (
        flight_id                   BIGINT IDENTITY(0,1) PRIMARY KEY,
        date                        DATE NOT NULL, 
        origin_city_id              BIGINT NOT NULL, 
        dest_city_id                BIGINT NOT NULL
    );    
"""

CREATE_CITIES = """
    CREATE TABLE public.cities (
        city_id                     BIGINT IDENTITY(0,1) PRIMARY KEY,
        city_name                   VARCHAR,  
        state_code                  VARCHAR
    ); 
"""

CREATE_WEATHER = """ 
    CREATE TABLE public.weather (
        city_name                   VARCHAR PRIMARY KEY, 
        country                     VARCHAR,
        city_id                     BIGINT,
        date                        DATE,
	    average_temp                DECIMAL(6,3), 
        average_temp_uncert         DECIMAL(6,3)
    );
"""

CREATE_DEMOGRAPHICS = """ 
    CREATE TABLE public.demographics (
        city_id                     VARCHAR PRIMARY KEY,
        median_age                  FLOAT,
        male_population             BIGINT,
        female_population           BIGINT,
        total_population            BIGINT,
        number_of_veterans          BIGINT,
        average_household_size      DECIMAL(6,3),
        race                        VARCHAR,
        count                       BIGINT
    );
"""


CREATE_TIME = """
    CREATE TABLE public.time (
          date                      DATE PRIMARY KEY,
          year                      INTEGER,
          month                     INTEGER,
          day_of_month              INTEGER
    );
"""


#### Inserts




CITIES_TABLE_INSERT_FLIGHTS  = ( """ 
    SELECT distinct city_name, state_code
    FROM ( SELECT origin_city_name, UPPER(split_part(origin_city_name,', ',1)) AS city_name,
            UPPER(split_part(origin_city_name, ', ',2)) AS state_code
            FROM staging_flights ) 
    """
)

CITIES_TABLE_INSERT_DEMOGRAPHICS = ("""
    SELECT distinct city_name, state_code 
    FROM (SELECT UPPER(city) AS city_name,
                 UPPER(state_code) AS state_code
          FROM staging_us_demographics)
    WHERE CONCAT(city_name, state_code) NOT IN (SELECT CONCAT(UPPER(city_name), UPPER(state_code)) FROM cities);
"""
)

DEMOGRAPHICS_TABLE_INSERT = """
    SELECT
        cities.city_id,
        staging_us_demographics.median_age,
        staging_us_demographics.male_population,
        staging_us_demographics.female_population,
        staging_us_demographics.total_population,
        staging_us_demographics.number_of_veterans,
        staging_us_demographics.average_household_size,
        staging_us_demographics.race,
        staging_us_demographics.count
    FROM staging_us_demographics
    JOIN cities
    ON UPPER(staging_us_demographics.city) = cities.city_name
    AND staging_us_demographics.state_code = cities.state_code
"""

TIME_TABLE_INSERT_FLIGHTS = """
    SELECT date, 
           extract(YEAR FROM date) as year, 
           extract(MONTH FROM date) as month, 
           extract(DAY from date) as day_of_month
    FROM staging_flights

"""

TIME_TABLE_INSERT_WEATHER = """
    SELECT dt as date, 
           extract(YEAR from date) as year, 
           extract(MONTH from date) as month,
           extract(DAY from date) as day_of_month
    FROM staging_weather
    WHERE date NOT IN (SELECT date FROM time) 
"""

FLIGHT_TABLE_INSERT = """
    SELECT sf.date, origin_cities.city_id, dest_cities.city_id 
    FROM staging_flights sf
    JOIN cities AS origin_cities ON UPPER(REPLACE(sf.origin_city_name,', ', '')) = CONCAT(origin_cities.city_name, origin_cities.state_code)
    JOIN cities AS dest_cities ON UPPER(REPLACE(sf.dest_city_name,', ', '')) = CONCAT(dest_cities.city_name, dest_cities.state_code)

"""

WEATHER_TABLE_INSERT = """
    SELECT sw.city, sw.country, cities.city_id, sw.dt, sw.average_temp, sw.average_temp_uncertainty
    FROM staging_weather sw 
    JOIN cities ON UPPER(sw.city) = cities.city_name 
    
"""

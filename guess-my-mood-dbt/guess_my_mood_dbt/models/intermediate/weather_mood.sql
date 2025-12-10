{{ 
  config(
    materialized='view',
    schema='intermediate',
    tags=['intermediate', 'weather', 'mood']
  )
}}

WITH weather_decoded AS (
  SELECT 
    *,
    -- CAST timestamp from STRING to TIMESTAMP
    TIMESTAMP(timestamp) as timestamp_ts,
    
    -- Extract time components from the CASTED timestamp
    DATE(TIMESTAMP(timestamp)) as date,
    EXTRACT(HOUR FROM TIMESTAMP(timestamp)) as hour_of_day,
    EXTRACT(DAYOFWEEK FROM TIMESTAMP(timestamp)) as day_of_week,
    EXTRACT(MONTH FROM TIMESTAMP(timestamp)) as month,
    
    -- Decode weathercode to human-readable conditions
    CASE 
      WHEN weathercode = 0 THEN 'clear'
      WHEN weathercode = 1 THEN 'mainly_clear'
      WHEN weathercode = 2 THEN 'partly_cloudy'
      WHEN weathercode = 3 THEN 'overcast'
      WHEN weathercode IN (45, 48) THEN 'foggy'
      WHEN weathercode IN (51, 53, 55) THEN 'drizzle'
      WHEN weathercode IN (61, 63, 65) THEN 'rainy'
      WHEN weathercode IN (71, 73, 75) THEN 'snowy'
      WHEN weathercode IN (77) THEN 'snow_grains'
      WHEN weathercode IN (80, 81, 82) THEN 'rain_showers'
      WHEN weathercode IN (85, 86) THEN 'snow_showers'
      WHEN weathercode IN (95) THEN 'thunderstorm'
      WHEN weathercode IN (96, 99) THEN 'thunderstorm_hail'
      ELSE 'unknown'
    END as weather_condition,
    
    -- Simplified conditions for mood analysis
    CASE 
      WHEN weathercode = 0 THEN 'sunny'
      WHEN weathercode IN (1, 2) THEN 'partly_cloudy'
      WHEN weathercode = 3 THEN 'cloudy'
      WHEN weathercode IN (45, 48) THEN 'foggy'
      WHEN weathercode IN (51, 53, 55, 61, 63, 65, 80, 81, 82) THEN 'rainy'
      WHEN weathercode IN (71, 73, 75, 77, 85, 86) THEN 'snowy'
      WHEN weathercode IN (95, 96, 99) THEN 'stormy'
      ELSE 'other'
    END as simplified_condition,
    
    -- Temperature categorization
    CASE 
      WHEN temperature_2m <= 0 THEN 'freezing'
      WHEN temperature_2m BETWEEN 0 AND 10 THEN 'very_cold'
      WHEN temperature_2m BETWEEN 10 AND 15 THEN 'cold'
      WHEN temperature_2m BETWEEN 15 AND 20 THEN 'cool'
      WHEN temperature_2m BETWEEN 20 AND 25 THEN 'warm'
      WHEN temperature_2m BETWEEN 25 AND 30 THEN 'hot'
      WHEN temperature_2m > 30 THEN 'very_hot'
      ELSE 'unknown'
    END as temperature_category,
    
    -- Cloud cover categorization
    CASE 
      WHEN cloudcover <= 20 THEN 'clear'
      WHEN cloudcover BETWEEN 21 AND 50 THEN 'partly_cloudy'
      WHEN cloudcover BETWEEN 51 AND 80 THEN 'mostly_cloudy'
      WHEN cloudcover > 80 THEN 'overcast'
      ELSE 'unknown'
    END as cloud_category

  FROM {{ source('staging', 'stg_weather_data') }}
),

mood_calculations AS (
  SELECT 
    *,
    -- START WITH NEUTRAL (0) AND ADJUST BASED ON PREFERENCES
    
    -- 1. TEMPERATURE SCORING (-5 to +5 range)
    CASE 
      WHEN temperature_category = 'warm' THEN 5    -- Perfect! You like warm temperatures
      WHEN temperature_category = 'hot' THEN 3     -- Good, but a bit too hot
      WHEN temperature_category = 'cool' THEN 1    -- Acceptable but not preferred
      WHEN temperature_category = 'cold' THEN -2   -- You don't like cold
      WHEN temperature_category = 'very_cold' THEN -4  -- You really don't like very cold
      WHEN temperature_category = 'freezing' THEN -5   -- Makes you sad
      WHEN temperature_category = 'very_hot' THEN 2    -- Too hot is less ideal
      ELSE 0
    END as temperature_score,
    
    -- 2. WEATHER CONDITION SCORING (-5 to +5 range)
    CASE 
      WHEN simplified_condition = 'sunny' THEN 5      -- Perfect! You like sunny weather
      WHEN simplified_condition = 'partly_cloudy' THEN 2   -- Acceptable
      WHEN simplified_condition = 'cloudy' THEN -2    -- You don't like cloudy
      WHEN simplified_condition = 'rainy' THEN -4     -- You don't like rain
      WHEN simplified_condition = 'snowy' THEN -4     -- You don't like snow (it's cold)
      WHEN simplified_condition = 'stormy' THEN -5    -- Definitely not pleasant
      WHEN simplified_condition = 'foggy' THEN -3     -- Not great for mood
      ELSE -1
    END as weather_score,
    
    -- 3. CLOUD COVER SCORING (-3 to +3 range)
    CASE 
      WHEN cloud_category = 'clear' THEN 3           -- Clear skies are best
      WHEN cloud_category = 'partly_cloudy' THEN 1   -- Some clouds okay
      WHEN cloud_category = 'mostly_cloudy' THEN -1  -- Too many clouds
      WHEN cloud_category = 'overcast' THEN -3       -- Overcast is depressing
      ELSE 0
    END as cloud_score,
    
    -- 4. TIME OF DAY SCORING (-2 to +2 range)
    CASE 
      WHEN hour_of_day BETWEEN 10 AND 16 THEN 2      -- Best daytime hours (sunlight)
      WHEN hour_of_day BETWEEN 7 AND 9 OR hour_of_day BETWEEN 17 AND 19 THEN 1  -- Morning/evening nice
      WHEN hour_of_day BETWEEN 20 AND 22 THEN 0      -- Evening okay
      WHEN hour_of_day BETWEEN 23 AND 6 THEN -1      -- Night time less ideal
      ELSE 0
    END as time_score,
    
    -- 5. PRECIPITATION PENALTY (-3 to 0)
    CASE 
      WHEN precipitation = 0 THEN 0                  -- No penalty for dry weather
      WHEN precipitation < 1 THEN -1                 -- Light rain
      WHEN precipitation < 5 THEN -2                 -- Moderate rain
      ELSE -3                                        -- Heavy rain/snow
    END as precipitation_penalty,
    
    -- 6. HUMIDITY ADJUSTMENT (-2 to +1)
    CASE 
      WHEN relativehumidity_2m BETWEEN 40 AND 60 THEN 1    -- Comfortable range
      WHEN relativehumidity_2m < 30 OR relativehumidity_2m > 70 THEN -1  -- Uncomfortable
      WHEN relativehumidity_2m > 80 THEN -2                -- Very humid, uncomfortable
      ELSE 0
    END as humidity_score

  FROM weather_decoded
),

final_mood AS (
  SELECT 
    *,
    -- CALCULATE TOTAL WEATHER MOOD (-10 to +10)
    (
      temperature_score + 
      weather_score + 
      cloud_score + 
      time_score + 
      precipitation_penalty + 
      humidity_score
    ) as weather_mood_raw,
    
    -- CLAMP TO -10 TO +10 RANGE
    GREATEST(-10, LEAST(10, 
      temperature_score + 
      weather_score + 
      cloud_score + 
      time_score + 
      precipitation_penalty + 
      humidity_score
    )) as weather_mood,
    
    -- CREATE MOOD CATEGORIES
    CASE 
      WHEN (
        temperature_score + 
        weather_score + 
        cloud_score + 
        time_score + 
        precipitation_penalty + 
        humidity_score
      ) >= 7 THEN 'excellent'
      WHEN (
        temperature_score + 
        weather_score + 
        cloud_score + 
        time_score + 
        precipitation_penalty + 
        humidity_score
      ) >= 3 THEN 'good'
      WHEN (
        temperature_score + 
        weather_score + 
        cloud_score + 
        time_score + 
        precipitation_penalty + 
        humidity_score
      ) >= -2 THEN 'neutral'
      WHEN (
        temperature_score + 
        weather_score + 
        cloud_score + 
        time_score + 
        precipitation_penalty + 
        humidity_score
      ) >= -6 THEN 'poor'
      ELSE 'bad'
    END as weather_mood_category

  FROM mood_calculations
)

SELECT 
  -- Original weather data
  timestamp,  -- Original string timestamp
  timestamp_ts as timestamp_converted,  -- Converted timestamp
  temperature_2m,
  apparent_temperature,
  weathercode,
  weather_condition,
  simplified_condition,
  cloudcover,
  cloud_category,
  precipitation,
  relativehumidity_2m,
  
  -- Time dimensions
  date,
  hour_of_day,
  day_of_week,
  month,
  
  -- Categorizations
  temperature_category,
  
  -- Individual mood components (for analysis)
  temperature_score,
  weather_score,
  cloud_score,
  time_score,
  precipitation_penalty,
  humidity_score,
  
  -- Final mood calculations
  weather_mood_raw,
  weather_mood,
  weather_mood_category,
  
  -- Derived flags
  CASE 
    WHEN weather_mood >= 7 THEN true 
    ELSE false 
  END as is_great_weather_mood,
  
  CASE 
    WHEN weather_mood <= -5 THEN true 
    ELSE false 
  END as is_poor_weather_mood,
  
  -- Ideal weather flag (sunny + warm)
  CASE 
    WHEN simplified_condition = 'sunny' AND temperature_category IN ('warm', 'hot') THEN true 
    ELSE false 
  END as is_ideal_weather_conditions,
  
  -- Poor weather flag (rainy/cold/stormy)
  CASE 
    WHEN simplified_condition IN ('rainy', 'snowy', 'stormy') OR temperature_category IN ('very_cold', 'freezing') THEN true 
    ELSE false 
  END as is_unfavorable_conditions,
  
  -- For trend analysis
  LAG(weather_mood) OVER (ORDER BY timestamp_ts) as prev_hour_weather_mood,
  LAG(temperature_2m) OVER (ORDER BY timestamp_ts) as prev_hour_temperature,
  
  -- Daily context
  AVG(weather_mood) OVER (PARTITION BY date) as daily_avg_weather_mood,
  MIN(temperature_2m) OVER (PARTITION BY date) as daily_min_temp,
  MAX(temperature_2m) OVER (PARTITION BY date) as daily_max_temp

FROM final_mood
ORDER BY timestamp_ts
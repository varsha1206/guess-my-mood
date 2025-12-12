{{ 
  config(
    materialized='table',
    schema='marts',
    tags=['marts', 'prediction', 'features', 'dashboard']
  )
}}

WITH 
-- Get all weather data
weather_data AS (
  SELECT 
    date as weather_date,
    hour_of_day as weather_hour,
    weather_mood,
    weather_mood_category,
    temperature_2m,
    apparent_temperature,
    weather_condition,
    simplified_condition,
    cloudcover,
    precipitation,
    relativehumidity_2m,
    temperature_category,
    is_great_weather_mood,
    is_poor_weather_mood
  FROM {{ ref('weather_mood') }}
),

-- Get ALL individual song plays from music_mood
music_data AS (
  SELECT 
    track_name as Content_Name,
    artist_name as Artist_Name,
    album_name,
    streamable,
    played_at,
    played_at_ts as music_timestamp,
    DATE(played_at_ts) as music_date,
    EXTRACT(HOUR FROM played_at_ts) as music_hour,
    EXTRACT(MINUTE FROM played_at_ts) as music_minute,
    
    -- Audio features
    danceability,
    energy,
    valence,
    tempo,
    loudness,
    popularity,
    explicit,
    duration_ms,
    track_genre,
    
    -- Audio features that exist in music_mood
    speechiness,
    acousticness,
    instrumentalness,
    liveness,
    
    -- Mood scores
    music_mood as song_mood,
    music_mood_category as song_mood_category,
    is_uplifting_music,
    is_depressing_music
    
  FROM {{ ref('music_mood') }}
  WHERE played_at_ts IS NOT NULL  -- Only remove completely null timestamps
)

-- Join music with weather using date and hour
SELECT 
  -- Time identifiers
  m.music_timestamp as Event_Start_Timestamp,
  m.music_timestamp as Event_Received_Timestamp,
  m.music_timestamp as Event_End_Timestamp,
  
  -- Song information
  m.Artist_Name,
  m.Content_Name,
  m.album_name,
  m.streamable,
  
  -- Audio features (from music_mood table)
  m.danceability,
  m.energy,
  m.valence,
  m.tempo,
  m.loudness,
  m.popularity,
  m.duration_ms,
  m.explicit,
  m.track_genre,
  m.speechiness,
  m.acousticness,
  m.instrumentalness,
  m.liveness,
  
  -- Individual mood scores
  m.song_mood,
  w.weather_mood,
  
  -- Weather data
  w.temperature_2m,
  w.apparent_temperature,
  w.weather_condition,
  w.simplified_condition,
  w.cloudcover,
  w.precipitation,
  w.relativehumidity_2m,
  
  -- Categorical features
  m.song_mood_category,
  w.weather_mood_category,
  w.temperature_category,
  w.is_great_weather_mood,
  w.is_poor_weather_mood,
  m.is_uplifting_music,
  m.is_depressing_music,
  
  -- Time dimension features
  m.music_date,
  m.music_hour,
  m.music_minute,
  EXTRACT(HOUR FROM m.music_timestamp) as event_hour,
  EXTRACT(DAYOFWEEK FROM m.music_timestamp) as event_day_of_week,
  EXTRACT(MONTH FROM m.music_timestamp) as event_month,
  EXTRACT(DAY FROM m.music_timestamp) as event_day,
  
  -- Join success indicator
  CASE WHEN w.weather_date IS NOT NULL THEN TRUE ELSE FALSE END as has_weather_data,
  
  -- Placeholder columns for training dataset compatibility
  '' as track_id,
  0 as key,
  0 as mode,
  0 as time_signature

FROM music_data m
LEFT JOIN weather_data w
  ON m.music_date = w.weather_date
  AND m.music_hour = w.weather_hour

ORDER BY Event_Start_Timestamp
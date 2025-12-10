{{ 
  config(
    materialized='view',
    schema='intermediate',
    tags=['intermediate', 'music', 'mood']
  )
}}

WITH cleaned_music_data AS (
  SELECT 
    track_name,
    artist_name,
    album_name,
    streamable,
    played_at,
    -- Clean track names for matching
    LOWER(TRIM(track_name)) as cleaned_track_name,
    LOWER(TRIM(artist_name)) as cleaned_artist_name,
    
    -- Parse the custom timestamp format: '08 Dec 2025, 05:31'
    CASE 
      WHEN played_at IS NOT NULL THEN
        TIMESTAMP(
          CONCAT(
            REGEXP_EXTRACT(played_at, r'(\d{4})'),
            '-',
            CASE 
              WHEN REGEXP_CONTAINS(played_at, r'Jan') THEN '01'
              WHEN REGEXP_CONTAINS(played_at, r'Feb') THEN '02'
              WHEN REGEXP_CONTAINS(played_at, r'Mar') THEN '03'
              WHEN REGEXP_CONTAINS(played_at, r'Apr') THEN '04'
              WHEN REGEXP_CONTAINS(played_at, r'May') THEN '05'
              WHEN REGEXP_CONTAINS(played_at, r'Jun') THEN '06'
              WHEN REGEXP_CONTAINS(played_at, r'Jul') THEN '07'
              WHEN REGEXP_CONTAINS(played_at, r'Aug') THEN '08'
              WHEN REGEXP_CONTAINS(played_at, r'Sep') THEN '09'
              WHEN REGEXP_CONTAINS(played_at, r'Oct') THEN '10'
              WHEN REGEXP_CONTAINS(played_at, r'Nov') THEN '11'
              WHEN REGEXP_CONTAINS(played_at, r'Dec') THEN '12'
              ELSE '01'
            END,
            '-',
            LPAD(REGEXP_EXTRACT(played_at, r'^(\d{1,2})'), 2, '0'),
            ' ',
            REGEXP_EXTRACT(played_at, r'(\d{2}:\d{2})'),
            ':00'
          )
        )
      ELSE NULL
    END as played_at_ts
    
  FROM {{ source('staging', 'stg_music_data') }}
  WHERE played_at IS NOT NULL
    AND track_name IS NOT NULL
),

cleaned_metadata AS (
  SELECT 
    track_name,
    artists,
    danceability,
    energy,
    valence,
    tempo,
    loudness,
    popularity,
    explicit,
    duration_ms,
    speechiness,
    acousticness,
    instrumentalness,
    liveness,
    track_genre,
    -- Clean track names for matching
    LOWER(TRIM(track_name)) as cleaned_track_name
    
  FROM {{ source('staging', 'stg_music_metadata') }}
  -- Remove duplicates in metadata by taking the first occurrence of each track name
  QUALIFY ROW_NUMBER() OVER (PARTITION BY LOWER(TRIM(track_name)) ORDER BY popularity DESC) = 1
),

-- Match music data with metadata (simple track name match)
matched_music AS (
  SELECT 
    m.track_name,
    m.artist_name,
    m.album_name,
    m.streamable,
    m.played_at,
    m.played_at_ts,
    -- Extract time components from parsed timestamp
    DATE(m.played_at_ts) as play_date,
    EXTRACT(HOUR FROM m.played_at_ts) as play_hour,
    EXTRACT(DAYOFWEEK FROM m.played_at_ts) as play_day_of_week,
    EXTRACT(MONTH FROM m.played_at_ts) as play_month,
    
    -- Get audio features from metadata
    mm.danceability,
    mm.energy,
    mm.valence,
    mm.tempo,
    mm.loudness,
    mm.popularity,
    mm.explicit,
    mm.duration_ms,
    mm.speechiness,
    mm.acousticness,
    mm.instrumentalness,
    mm.liveness,
    mm.track_genre,
    
    -- Convert duration from ms to minutes
    ROUND(mm.duration_ms / 60000, 2) as duration_minutes,
    
    -- Flag for successful match
    CASE WHEN mm.track_name IS NOT NULL THEN true ELSE false END as matched_with_metadata
    
  FROM cleaned_music_data m
  LEFT JOIN cleaned_metadata mm
    ON m.cleaned_track_name = mm.cleaned_track_name
),

music_mood_scoring AS (
  SELECT 
    *,
    
    -- 1. ENERGY SCORING (-3 to +5)
    CASE 
      WHEN energy >= 0.8 THEN 5
      WHEN energy >= 0.6 THEN 3
      WHEN energy >= 0.4 THEN 1
      WHEN energy >= 0.2 THEN -1
      ELSE -3
    END as energy_score,
    
    -- 2. VALENCE SCORING (-5 to +5)
    CASE 
      WHEN valence >= 0.8 THEN 5
      WHEN valence >= 0.6 THEN 3
      WHEN valence >= 0.4 THEN 1
      WHEN valence >= 0.2 THEN -2
      ELSE -5
    END as valence_score,
    
    -- 3. DANCEABILITY SCORING (-2 to +3)
    CASE 
      WHEN danceability >= 0.7 THEN 3
      WHEN danceability >= 0.5 THEN 1
      WHEN danceability >= 0.3 THEN -1
      ELSE -2
    END as danceability_score,
    
    -- 4. TEMPO SCORING (-2 to +3)
    CASE 
      WHEN tempo BETWEEN 100 AND 140 THEN 3
      WHEN tempo BETWEEN 80 AND 100 THEN 1
      WHEN tempo BETWEEN 140 AND 180 THEN 2
      WHEN tempo < 60 THEN -2
      WHEN tempo BETWEEN 60 AND 80 THEN -1
      ELSE 0
    END as tempo_score,
    
    -- 5. GENRE SCORING (-3 to +3)
    CASE 
      WHEN LOWER(track_genre) IN ('pop', 'dance', 'disco', 'funk') THEN 3
      WHEN LOWER(track_genre) IN ('rock', 'alternative', 'indie') THEN 1
      WHEN LOWER(track_genre) IN ('hip-hop', 'rap', 'r-n-b') THEN 2
      WHEN LOWER(track_genre) IN ('jazz', 'blues') THEN 0
      WHEN LOWER(track_genre) IN ('classical') THEN -1
      WHEN LOWER(track_genre) IN ('metal', 'hard-rock') THEN -2
      WHEN LOWER(track_genre) IN ('sad', 'emo') THEN -3
      ELSE 0
    END as genre_score,
    
    -- 6. ACOUSTICNESS SCORING (-2 to +2)
    CASE 
      WHEN acousticness >= 0.8 THEN -1
      WHEN acousticness >= 0.5 THEN 0
      WHEN acousticness >= 0.2 THEN 1
      ELSE 2
    END as acousticness_score,
    
    -- 7. TIME OF LISTENING SCORING (-1 to +2)
    CASE 
      WHEN play_hour BETWEEN 6 AND 10 THEN 2
      WHEN play_hour BETWEEN 16 AND 20 THEN 1
      WHEN play_hour BETWEEN 22 AND 5 THEN -1
      ELSE 0
    END as time_listening_score,
    
    -- 8. POPULARITY BONUS (-1 to +2)
    CASE 
      WHEN popularity >= 80 THEN 2
      WHEN popularity >= 60 THEN 1
      WHEN popularity >= 40 THEN 0
      WHEN popularity >= 20 THEN -1
      ELSE 0
    END as popularity_score

  FROM matched_music
  WHERE matched_with_metadata = true  -- Only score tracks with metadata
),

final_mood_calculation AS (
  SELECT 
    *,
    -- CALCULATE TOTAL MUSIC MOOD (-10 to +10)
    (
      energy_score +
      valence_score +
      danceability_score +
      tempo_score +
      genre_score +
      acousticness_score +
      time_listening_score +
      popularity_score
    ) as music_mood_raw,
    
    -- CLAMP TO -10 TO +10 RANGE
    GREATEST(-10, LEAST(10, 
      energy_score +
      valence_score +
      danceability_score +
      tempo_score +
      genre_score +
      acousticness_score +
      time_listening_score +
      popularity_score
    )) as music_mood,
    
    -- CREATE MOOD CATEGORIES
    CASE 
      WHEN (
        energy_score +
        valence_score +
        danceability_score +
        tempo_score +
        genre_score +
        acousticness_score +
        time_listening_score +
        popularity_score
      ) >= 7 THEN 'uplifting'
      WHEN (
        energy_score +
        valence_score +
        danceability_score +
        tempo_score +
        genre_score +
        acousticness_score +
        time_listening_score +
        popularity_score
      ) >= 3 THEN 'positive'
      WHEN (
        energy_score +
        valence_score +
        danceability_score +
        tempo_score +
        genre_score +
        acousticness_score +
        time_listening_score +
        popularity_score
      ) >= -2 THEN 'neutral'
      WHEN (
        energy_score +
        valence_score +
        danceability_score +
        tempo_score +
        genre_score +
        acousticness_score +
        time_listening_score +
        popularity_score
      ) >= -6 THEN 'melancholic'
      ELSE 'depressing'
    END as music_mood_category

  FROM music_mood_scoring
)

SELECT 
  -- Original music data
  track_name,
  artist_name,
  album_name,
  streamable,
  played_at,
  played_at_ts,
  play_date,
  play_hour,
  play_day_of_week,
  play_month,
  
  -- Audio features from metadata
  danceability,
  energy,
  valence,
  tempo,
  loudness,
  popularity,
  explicit,
  duration_ms,
  duration_minutes,
  speechiness,
  acousticness,
  instrumentalness,
  liveness,
  track_genre,
  
  -- Individual mood component scores
  energy_score,
  valence_score,
  danceability_score,
  tempo_score,
  genre_score,
  acousticness_score,
  time_listening_score,
  popularity_score,
  
  -- Final mood calculations
  music_mood_raw,
  music_mood,
  music_mood_category,
  
  -- Simple flags for analysis
  CASE 
    WHEN music_mood >= 7 THEN true 
    ELSE false 
  END as is_uplifting_music,
  
  CASE 
    WHEN music_mood <= -5 THEN true 
    ELSE false 
  END as is_depressing_music,
  
  -- Time-based patterns
  CASE 
    WHEN play_hour BETWEEN 6 AND 12 THEN 'morning'
    WHEN play_hour BETWEEN 12 AND 18 THEN 'afternoon'
    WHEN play_hour BETWEEN 18 AND 22 THEN 'evening'
    ELSE 'night'
  END as time_of_day_category

FROM final_mood_calculation
ORDER BY played_at_ts
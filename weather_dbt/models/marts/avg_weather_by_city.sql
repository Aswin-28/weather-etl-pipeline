with stg as (
    select * from {{ ref('stg_weather') }}
)

select
    city,
    round(avg(temperature)::numeric, 2)  as avg_temperature,
    round(avg(feels_like)::numeric, 2)   as avg_feels_like,
    round(avg(humidity)::numeric, 2)     as avg_humidity,
    round(avg(wind_speed)::numeric, 2)   as avg_wind_speed,
    count(*)                             as total_readings,
    min(recorded_at)                     as first_recorded,
    max(recorded_at)                     as last_recorded
from stg
group by city
order by avg_temperature desc
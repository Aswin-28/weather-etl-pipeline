with source as (
    select * from weather_data
),

staged as (
    select
        city,
        temperature,
        feels_like,
        humidity,
        weather,
        wind_speed,
        timestamp::timestamp as recorded_at
    from source
    where city is not null
      and temperature is not null
)

select * from staged
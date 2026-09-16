-- Finite negative readings remain measurements; no physical-validity bounds are inferred.
SELECT
    source_date,
    avg(tp2) AS avg_tp2,
    min(tp2) AS min_tp2,
    max(tp2) AS max_tp2,
    avg(tp3) AS avg_tp3,
    avg(h1) AS avg_h1,
    avg(dv_pressure) AS avg_dv_pressure,
    avg(reservoirs) AS avg_reservoirs,
    avg(oil_temperature) AS avg_oil_temperature,
    avg(motor_current) AS avg_motor_current
FROM metropulse_dev.metropt3_curated
WHERE year = '2020'
  AND month = '02'
GROUP BY source_date
ORDER BY source_date;

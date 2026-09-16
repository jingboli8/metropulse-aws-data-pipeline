SELECT
    source_date,
    avg(CASE WHEN comp THEN 1.0 ELSE 0.0 END) AS comp_active_rate,
    avg(CASE WHEN dv_electric THEN 1.0 ELSE 0.0 END) AS dv_electric_active_rate,
    avg(CASE WHEN towers THEN 1.0 ELSE 0.0 END) AS towers_active_rate,
    avg(CASE WHEN mpg THEN 1.0 ELSE 0.0 END) AS mpg_active_rate,
    avg(CASE WHEN lps THEN 1.0 ELSE 0.0 END) AS lps_active_rate,
    avg(CASE WHEN pressure_switch THEN 1.0 ELSE 0.0 END) AS pressure_switch_active_rate,
    avg(CASE WHEN oil_level THEN 1.0 ELSE 0.0 END) AS oil_level_active_rate,
    avg(CASE WHEN caudal_impulses THEN 1.0 ELSE 0.0 END) AS caudal_impulses_active_rate
FROM metropulse_dev.metropt3_curated
WHERE year = '2020'
  AND month = '02'
GROUP BY source_date
ORDER BY source_date;

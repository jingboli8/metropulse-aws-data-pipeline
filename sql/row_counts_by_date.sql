-- Development reference: metropulse_dev.metropt3_curated.
-- Change only the environment-scoped database name for another deployment.
SELECT
    source_date,
    count(record_index) AS observation_count
FROM metropulse_dev.metropt3_curated
WHERE year = '2020'
  AND month = '02'
GROUP BY source_date
ORDER BY source_date;

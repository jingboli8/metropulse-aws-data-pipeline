# MetroPT-3 data source

## Official source and attribution

- **Official title:** MetroPT-3 Dataset
- **Creators:** Narjes Davari, Bruno Veloso, Rita Ribeiro, and Joao Gama
- **Publisher:** UCI Machine Learning Repository
- **UCI dataset ID:** 791
- **Official page:** <https://archive.ics.uci.edu/dataset/791/metropt%2B3%2Bdataset>
- **DOI:** <https://doi.org/10.24432/C5VW3R>
- **Accessed:** 2026-09-04
- **Dataset license:** Creative Commons Attribution 4.0 International (CC BY 4.0)

Suggested attribution, following UCI's citation:

> Davari, N., Veloso, B., Ribeiro, R., & Gama, J. (2021). MetroPT-3 Dataset
> [Dataset]. UCI Machine Learning Repository. <https://doi.org/10.24432/C5VW3R>.

The UCI page explicitly marks the dataset as CC BY 4.0 and says sharing and adaptation
are permitted for any purpose with appropriate credit. It also answers **“no”** to the
question of whether the dataset contains data that might be considered sensitive.

The dataset license applies to the dataset. It does not automatically determine the
license of MetroPulse's future source code. A code license must be selected separately
during a later project phase.

## What the records represent

The records are real multivariate telemetry from analogue and digital sensors installed
on a metro train compressor's Air Production Unit (APU), collected in an operational
railway context. Signals cover pressures, oil temperature, motor current, valves, tower
selection, pressure/oil switches, and air-flow impulses. These are industrial-equipment
telemetry records; they are not manufacturing production-line data.

The CSV is unlabeled. UCI supplies failure-report windows separately:

| Window | Start | End | Failure | UCI notes |
|---|---:|---:|---|---|
| 1 | 2020-04-18 00:00 | 2020-04-18 23:59 | Air leak | High stress |
| 2 | 2020-05-29 23:30 | 2020-05-30 06:00 | Air leak | High stress; the maintenance text on UCI says “30Apr at 12:00,” which appears internally inconsistent and is preserved here as a source issue |
| 3 | 2020-06-05 10:00 | 2020-06-07 14:30 | Air leak | High stress; maintenance on 2020-06-08 at 16:00 |
| 4 | 2020-07-15 14:30 | 2020-07-15 19:00 | Air leak | High stress; maintenance on 2020-07-16 at 00:00 |

These windows are documented in the **Additional Information / Failure Information**
section of the official UCI dataset page. The downloaded archive also includes UCI's
`Data Description_Metro.pdf`. They are contextual reports, not row-level labels added to
the CSV.

## Verified download provenance

- Download URL:
  <https://archive.ics.uci.edu/static/public/791/metropt%2B3%2Bdataset.zip>
- ZIP SHA-256:
  `aab991a970e58210de853bb8078ce0e63abb4d9412fdc5c79792dae3d8e1721a`
- ZIP size: 218,381,995 bytes (208.265 MiB)
- CSV member: `MetroPT3(AirCompressor).csv`, 218,300,507 compressed bytes and
  218,300,507 uncompressed bytes (the ZIP stores this member without compression)
- Description member: `Data Description_Metro.pdf`, 81,208 compressed and uncompressed
  bytes

The full source archive, extracted CSV, description PDF, and all derived data remain
under ignored `data/` paths and will not be committed to Git.

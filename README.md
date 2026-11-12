# Weather Intelligence Lab (Assignment 5)

This project ingests weather data (observations, forecasts, alerts, and station metadata), applies data-quality checks, and performs various analyses using PySpark. The notebook cleans and enriches the raw data, computes summary statistics and rolling-window measures, evaluates forecast accuracy, and explores the timing of issued alerts relative to extreme weather readings. All outputs are saved as Parquet datasets for further use. 

## Repository Structure

```
Weather_Intelligence/
├── Data/
│   ├── raw/                # Raw input CSV files (observations, forecasts, alerts, stations)
│   └── processed/
│       ├── dimension/      # (optional) Processed dimension tables (e.g. stations)
│       └── analytics/      # Curated analytical output tables (Parquet files)
├── notebooks/
│   └── Data_exploration.ipynb  # Main PySpark analysis notebook
├── README.md
└── requirements.txt       # (Optional) Python dependencies
```

The `Data/raw/` directory must contain the four CSV files provided for the assignment:
- **observations.csv** (raw weather sensor readings)
- **forecasts.csv** (forecast data from external source)
- **alerts.csv** (issued alerts)
- **stations.csv** (station metadata with ID, name, state, etc.)

The notebook reads these files using PySpark, cleans and transforms the data, and writes results to `Data/processed/analytics/`. Make sure the paths in the notebook match your local directory structure.

## Prerequisites and Setup

- **Operating System:** Windows (instructions assume Windows; similar steps apply on macOS/Linux).
- **Java 8+**: Install Java runtime, as Spark requires it. Ensure `java -version` is in your PATH.
- **Python 3.8+**: The code was tested with Python 3.8; newer versions should work.
- **Apache Spark:** Install Apache Spark (3.x) compatible with your Python version. Set `SPARK_HOME` and add `$SPARK_HOME/bin` to PATH.
- **Python packages:** Install required packages in the same Python environment used by your notebook. At minimum:
  - `pyspark`
  - `matplotlib`
  - `pandas` (if used in any optional analysis)
  
  Example (in a Jupyter cell or command prompt):
  ```bash
  pip install pyspark matplotlib pandas
  ```
- **Jupyter Notebook / VS Code:** The analysis is provided as a Jupyter notebook (`Data_exploration.ipynb`). You can open and run this in Jupyter or VS Code (with the Python extension). The code uses PySpark, so ensure the notebook is launched with the Python interpreter where PySpark is installed. 

  In VS Code, you may need to configure the Spark driver memory or select the Python environment. On Windows, you can use `findspark` or set the `PYSPARK_DRIVER_PYTHON` environment variable if needed. (The notebook example does not explicitly show Spark session creation, but it should begin with creating a `SparkSession`.)

- **Matplotlib:** If running the visualization cells, ensure `matplotlib` is installed. A sample cell uses `%pip install matplotlib` to add it.

**Note:** Adjust file paths in the notebook if your directory structure differs. Also ensure your Spark configuration (master, driver memory) is set appropriately for your system.

## Data Processing and Quality

1. **Raw Data Ingestion:** The notebook reads the raw CSV files into Spark DataFrames. It expects the four inputs mentioned above in `Data/raw/`.

2. **Data Cleaning and Quarantine:** The raw data are validated and cleaned. Key steps include:
   - **Station ID Validation:** Records with station IDs not found in `stations.csv` are quarantined. A total of **374** records were quarantined for issues (including unmatched station IDs, invalid dates, out-of-range values, etc.). The remaining **4,057** records were retained from the original **4,431**.
   - **Duplicate Removal:** Exact-duplicate observation records (same values in all fields) were identified and removed. In this dataset, **3 duplicate** rows were found and dropped.
   - **Value Ranges:** Measurements outside plausible ranges were adjusted or removed:
     - **Temperature (°C):** Typically allowed range −50 to +60. (This dataset contained only valid temps after filtering.)
     - **Humidity (%):** Clipped to [0,100]. Values above 100 or below 0 were quarantined. 
     - **Rainfall (mm) and Wind (kph):** Negative values were set to 0 (since negative rainfall or wind is not physically meaningful). 
     - **Alert Fields:** Alerts with missing or invalid severity were handled (the notebook does not explicitly remove any valid alerts for missing fields, but does enforce data types).
   - **Date/Time Fields:** The notebook uses Spark to parse dates. If an observation or alert had an invalid timestamp, it would be flagged (none were reported after filtering).
   - **Quarantine Data:** All filtered-out records (with their reasons) were retained in separate quarantine tables (not saved as final outputs) for audit. 

3. **Data Enrichment:** Cleaned observation and alert records are **enriched** by joining with the station metadata:
   - The station table (`stations.csv`) provides station name and state. This was joined (via `station_id`) to add `station_name` and `state` fields to both observations and alerts.
   - After the join, we drop redundant fields like country, elevation, etc., keeping only `station_id`, `station_name`, `state` and the weather/alert fields.

4. **Processed Master Tables:** The resulting DataFrames (e.g. `observations_enriched` and `alerts_enriched`) contain consistent, cleaned data ready for analysis.

## Analytical Transformations and Findings

The notebook performs several analyses using Spark transformations and window functions. Key results include:

- **Station Temperature Summary:** Aggregated by station to compute:
  - Mean daily temperature
  - Lowest recorded temperature
  - Highest recorded temperature
  - Number of days with data

  *Key findings:*  
  - *Highest mean temperature:* Mumbai (~30.03°C).  
  - *Lowest mean temperature:* Warangal (~29.24°C).  
  - All eight stations have similar average temperatures (~29–30°C) over June 2026.

- **Daily Weather Report:** One row per station-day (`station_id`, `station_name`, `observation_date`) with:
  - `avg_temperature_c`
  - `min_temperature_c`, `max_temperature_c`
  - `rainfall_reading_mm` (sum of rainfall readings if available)
  - Other daily aggregates as needed.

  Using this table, we can identify “hot days” (where daily average ≥ 40°C). In this dataset, **109 out of 236 station-day records** met that threshold. These daily records feed into the next analysis.

- **Consecutive Hot Periods:** The notebook identifies contiguous multi-day sequences at each station where `avg_temperature_c ≥ 40°C`. A period ends when the next day’s average drops by more than 10°C (or the station has no reading). The summary (`hot_period_summary`) lists each period’s start and end date and duration.  
  *Example:* If a station is ≥40°C on June 3–5 and then drops, that is one hot period of 3 days. The code uses a Spark window partitioned by station, comparing each row’s date with the previous one to detect breaks.

- **Forecast vs. Observation Matching:** We evaluate how well the provided forecasts match the actual observations.  
  - **Exact timestamp matching:** Initially, forecasts were joined to observations only if timestamps matched exactly. This covered 329 out of 885 forecast-hour records (37.2% coverage).  
  - **Tolerant matching (±1, ±2, ±3 hours):** To improve coverage, forecasts were matched to the closest observation within a 3-hour window (one-to-one pairing only). This increased coverage to 693 out of 885 (78.3%) when using ±3 hours. (No observation or forecast was reused thanks to a ranking-and-filtering strategy.)  
  - **Error Metrics:** On the 693 matched pairs, we computed:
    - *Temperature MAE:* ~7.41°C  
    - *Temperature RMSE:* ~9.12°C  
    - *Temperature bias:* ~−0.42°C (forecasts slightly colder on average)  
    - *Rainfall-reading MAE:* ~22.48 mm (note: this compares one-hour forecast values to the observed hourly rainfall reading; it is not a validated rainfall accumulation comparison).  
  - **Station-level errors:** Performance varies by station (see output `tolerant_station_metrics`). For example, Pune had the lowest MAE (~6.76°C) and Warangal the highest (~8.27°C).

- **Rolling 3-Day Averages:** We computed a 3-calendar-day rolling average of daily temperature for each station (using Spark’s `rangeBetween(-2*24*60*60, 0)` window). This accounts for any missing days (the window uses a 72-hour range). We also record how many days contributed to each average. This smoothes out day-to-day variability; for stations with no gaps, each 3-day average uses exactly 3 days.

- **Latest Valid Observation:** For each station, we identified the single most recent observation (by timestamp) that has a non-null temperature. This gives a snapshot of the latest reading available in the data. (Other fields may still be null, but we require a valid temperature to count the observation.)

- **Alert Timing vs. Extreme Weather:** We explored the timing of issued alerts relative to extreme weather events (defined by exploratory thresholds). First, we aggregated the enriched observations into hourly values (averaging temperature, rainfall, wind). We flagged *extreme events* as follows:
  - **Heat:** hourly avg temp ≥ 40°C  
  - **Heavy rain:** hourly avg rainfall reading ≥ 50 mm  
  - **High wind:** hourly avg wind speed ≥ 50 kph  

  We found **794** station-hour records meeting any extreme condition. Then, for each alert (e.g. a “heat” alert at a station), we searched for the nearest matching extreme event (same station and type) within ±3 hours of the alert time. We kept only the single closest event per alert. Alerts were categorized as:
  - *“before”* (extreme event occurred after the alert)  
  - *“during”* (alert and extreme event in the same clock hour)  
  - *“after”* (extreme event occurred before the alert)  
  - *“no_extreme_reading_within_3h”* (no matching event in the window)  

  Among 189 total alerts:
  - 73 had a matched extreme event (36 after, 26 before, 11 during)
  - 116 had no matching extreme reading within ±3 hours.

  This analysis is purely exploratory and does not imply whether alerts were “correct” or timely – only how they align with our defined extreme thresholds.

## Spark SQL / API and Window Functions

All data manipulations are implemented in PySpark using the DataFrame (Dataset) API and built-in SQL functions from `pyspark.sql.functions`. Key uses of SQL/window features include:

- **Aggregations:** Basic groupBy and aggregation (mean, min, max, count) to build summary tables (station summary, daily aggregates).
- **Window Functions:** Several window operations are used:
  - **`lag` / difference:** To compare each day’s average temperature to the previous day for splitting hot periods.
  - **`dense_rank` or `row_number`:** To identify consecutive hot-day sequences and assign period IDs.
  - **Time-range window:** `.rangeBetween(-2*24*3600, 0)` to compute the rolling 3-day average temperature (3 calendar days).
  - **`row_number` over descending time:** To pick the latest observation per station.
- **Joins:** Both broadcast (small tables) and regular joins are used:
  - The station metadata table is small (8 rows) and could be broadcast-joined to observations and alerts for enrichment.
  - Forecast-observation matching uses a join on station ID with a time condition; a window then picks the best one-to-one matches.
  - Alerts to extreme events join on station ID and alert type, with a timestamp proximity filter.

We primarily used the PySpark DataFrame API. Equivalent Spark SQL queries could be written for any of these steps if desired; key results (e.g. daily aggregates, summary tables) were spot-checked via SQL to ensure consistency.

## Performance and Execution Notes

- The dataset is relatively small (a few thousand rows), so performance is not a major issue. However, we followed best practices:
  - **Caching:** After heavy filtering, the cleaned `observations_clean` DataFrame was cached in memory to avoid re-reading from disk on each action.
  - **Partitioning:** Most joins and windows partitioned by `station_id`, which is a natural small cardinality and kept partitions manageable.
  - **Broadcast Joins:** Small dimension tables (stations) were effectively broadcast to all executors to speed up the enrichment joins.
  - **Lazy Execution:** All transformations are lazily evaluated; actions like `.show()` and `.count()` trigger the actual computation.
  - **SQL EXPLAIN:** (Not shown) One could use `df.explain()` to inspect the execution plan. The current approach largely uses “sort-aggregate” for grouping, “rangeJoin/broadcastHashJoin” for joins, and window functions that require sorting within partitions.

No major performance bottlenecks were encountered. For very large datasets, additional tuning (caching intermediate results, optimizing shuffles, etc.) would be recommended.

## Outputs (Parquet files)

The notebook saves the following curated analytical tables under `Data/processed/analytics/` (in Parquet format):

- **station_temperature_summary.parquet** — One row per station: station_id, station_name, state, mean_daily_temperature_c, lowest_recorded_temperature_c, highest_recorded_temperature_c, days_with_temperature_data.  
  *(Rows: 8)*

- **daily_weather.parquet** — One row per station-day: station_id, station_name, state, observation_date, avg_temperature_c, min_temperature_c, max_temperature_c, rainfall_reading_sum_mm (if computed), etc.  
  *(Rows: 236 = 8 stations × ~29–30 days)*

- **hot_period_summary.parquet** — Consecutive hot-day periods (avg_temperature_c ≥ 40°C): station_id, station_name, start_date, end_date, days_in_period.  
  *(Rows: 27 periods found)*

- **temperature_evaluation.parquet** — Exact-timestamp forecast vs observation matches (used as a baseline for coverage). Contains matched records with forecast vs observed temperature and rainfall.  
  *(Rows: 327 pairs matched exactly)*

- **alerts_weather_context.parquet** — Each alert (with station_name, type, time) joined to the closest observation (weather context) at the same hour (if any). This shows the weather conditions when each alert was issued.  
  *(Rows: 189 alerts)*

- **tolerant_forecast_evaluation.parquet** — The ±3-hour matched forecast-observation pairs (one-to-one). Includes forecast vs observed temperature and rainfall, and the time difference.  
  *(Rows: 693)*

- **rolling_3day_temperature.parquet** — For each station-day, the rolling 3-day average temperature and the count of days in that window.  
  *(Rows: 236)*

- **latest_valid_observation.parquet** — The most recent (latest timestamp) valid observation for each station. Fields include station, timestamp, temperature, humidity, rainfall, wind, condition, etc.  
  *(Rows: 8)*

- **alerts_extreme_timing.parquet** — For each alert, the nearest extreme weather event match (if any) and its timing category (before, during, after, or no match).  
  *(Rows: 189)*

Each Parquet file was validated by re-loading and checking row counts & schema. All saved tables match the in-notebook DataFrames.

## Running the Notebook

1. **Open the notebook:** In VS Code (with Python extension) or JupyterLab/Notebook.  
2. **Set file paths:** Ensure the raw data paths in the first cells point to your `Data/raw/` folder. The code may use relative paths (e.g. `Data/raw/observations.csv`), so launch the notebook from the project root.  
3. **Kernel/Interpreter:** Select the Python environment where PySpark and necessary packages are installed.  
4. **Run cells in order:** The notebook is linear; run each cell from top to bottom. Key steps:
   - Initialize SparkSession and any configurations.
   - Read and clean raw data (the first few cells).
   - Perform summaries and analyses (middle cells).
   - Save outputs to Parquet (final cells).
   - Optionally, generate the visualizations at the end (requires `matplotlib`).  
5. **Check outputs:** After running, the `Data/processed/analytics/` directory will contain the Parquet files listed above. You can inspect them (e.g. with `spark.read.parquet`) or load them into any Spark/SQL engine.

No external API keys or network access are required; all data are local files. If running on a cluster, ensure the driver and workers can access the same data path.

## Key Assumptions and Limitations

- **Thresholds:** The definitions of “hot day” (≥40°C), “heavy rain” (≥50 mm/hr), and “high wind” (≥50 kph) are *exploratory* thresholds for this analysis. They are not official alert criteria. Conclusions about alerts vs. events are purely relative to these chosen cutoffs.
- **Rainfall Comparisons:** We compared forecast `rain_mm` (a one-hour forecast accumulation) to the observed `rainfall_reading_mm` (one-hour reading). We do *not* claim validated accuracy for multi-day rain totals. The MAE of ~22.5 mm should be interpreted cautiously, as measurement intervals may differ.
- **Missing Data:** Some station-days or hours are missing data (e.g. no observations at certain times). Window calculations (like the 3-day average) skip missing days automatically via the time-based window. The latest-observation method simply picks the last available timestamp, which may vary by station.
- **Time Zones:** All times are assumed to be in the same zone. We used `date_trunc('hour', observed_at)` to align hourly aggregates.
- **Data Freshness:** The notebook assumes the raw data reflect a static snapshot (June 2026). In a real-time system, one might periodically append new data and re-run analyses.
- **Scalability:** The code is not optimized for very large datasets. For example, joining all forecasts to all observations is done in-memory. For larger data, consider partitioning by date or using Spark’s broadcast/partition hints.

## Tests and Validation

- **Row-Count Checks:** After each major save (e.g. Parquet outputs), the notebook re-reads the file and compares row counts to the in-memory DataFrame to ensure completeness.
- **Schema Checks:** The schema of each output Parquet is matched against the expected fields. A failure would indicate a transformation error.
- **Spot Checks:** We manually examined some summary results (e.g. station means, a few alert matches) by running Spark SQL queries. These confirmed the logic of the DataFrame steps.
- **Edge Cases:** The code handles possible edge cases like ties in window ranking by including a secondary sort (e.g. order by timestamp). If two extreme events were equally distant in time, the earlier one was chosen. Alerts with no extreme match were explicitly labeled.

## Deliverables Checklist

- [x] Cleaned and enriched dataset (observations and alerts with station info)  
- [x] Data quality & quarantine report (documents records removed/flagged)  
- [x] `station_temperature_summary.parquet` (8 rows)  
- [x] `daily_weather.parquet` (236 rows)  
- [x] `hot_period_summary.parquet` (27 rows)  
- [x] `temperature_evaluation.parquet` (327 rows, exact timestamp matches)  
- [x] `alerts_weather_context.parquet` (189 rows)  
- [x] `tolerant_forecast_evaluation.parquet` (693 rows, ±3h matches)  
- [x] `rolling_3day_temperature.parquet` (236 rows)  
- [x] `latest_valid_observation.parquet` (8 rows)  
- [x] `alerts_extreme_timing.parquet` (189 rows)  
- [x] Visualisations (included in notebook; e.g. temperature bar chart, trend lines)  
- [x] Findings and observations documented in the notebook (and summarized above)


# Container Planning and No-Cycle Analysis Guidelines

## Project Overview

This project analyzes container planning data with a focus on identifying and understanding no-cycle patterns (inducted packages that were not planned) at delivery stations. The analysis integrates location mapping for timezone-aware processing and provides insights into planning effectiveness across stations and time periods.

## Redshift Querying via Bedrock Knowledge Base

To query Redshift data using the AWS Bedrock knowledge base:

1. Use the `aws bedrock-agent-runtime retrieve` command
2. Format queries as JSON files with the `text` field containing the natural language question
3. Create the JSON file with proper escaping: `echo '{"text": "your query here"}' > query.json`
4. Submit the query using: `aws bedrock-agent-runtime retrieve --knowledge-base-id RD2ZPKUAUT --retrieval-query file://query.json`
5. The knowledge base ID for Redshift is `RD2ZPKUAUT`

## Example Query

```bash
# Create the query file
echo '{"text": "show all tables in the database"}' > query.json

# Submit the query to Bedrock
aws bedrock-agent-runtime retrieve --knowledge-base-id RD2ZPKUAUT --retrieval-query file://query.json
```

## Core No-Cycle Analysis Queries

### Base Query with Daily Aggregation

```sql
SELECT
    "date" AS ofd_date,
    station_code,
    SUM(is_planned) AS is_planned,
    SUM(is_inducted) AS is_inducted,
    SUM(is_inducted_as_planned) AS is_inducted_as_planned,
    SUM(is_planned_not_inducted) AS is_planned_not_inducted,
    SUM(is_inducted_not_planned) AS is_inducted_not_planned,
    CASE
        WHEN SUM(is_inducted) = 0 THEN 0
        ELSE ROUND(100.0 * SUM(is_inducted_not_planned) / SUM(is_inducted), 2)
    END AS pct_inducted_not_planned,
    CASE
        WHEN SUM(is_inducted) = 0 THEN 0
        ELSE ROUND(100.0 * SUM(is_planned) / SUM(is_inducted), 2)
    END AS pct_planned_vs_inducted
FROM "amzlanalytics"."amzlcore"."amzl_routing2_container_pkg_na" ROUTE
INNER JOIN amzl_mapping MAP ON 1=1
    AND ROUTE.region_id = 1
    AND ROUTE."date" >= '2025-03-01'::TIMESTAMP
    AND MAP.node = ROUTE.station_code
WHERE 1=1
GROUP BY 1, 2
ORDER BY "date", station_code;
```

### Creating a Temp Table for Base Calculations

```sql
-- Create a temporary table with base calculations
CREATE TEMPORARY TABLE temp_data (
    date DATE,
    station_code TEXT,
    is_planned INT,
    is_inducted INT,
    is_inducted_as_planned INT,
    is_planned_not_inducted INT,
    is_inducted_not_planned INT,
    pct_inducted_not_planned NUMERIC
) ON COMMIT DROP;

INSERT INTO temp_data
SELECT
    "date",
    station_code,
    SUM(is_planned) AS is_planned,
    SUM(is_inducted) AS is_inducted,
    SUM(is_inducted_as_planned) AS is_inducted_as_planned,
    SUM(is_planned_not_inducted) AS is_planned_not_inducted,
    SUM(is_inducted_not_planned) AS is_inducted_not_planned,
    CASE
        WHEN SUM(is_inducted) = 0 THEN 0
        ELSE ROUND(100.0 * SUM(is_inducted_not_planned) / SUM(is_inducted), 2)
    END AS pct_inducted_not_planned
FROM "amzlanalytics"."amzlcore"."amzl_routing2_container_pkg_na" ROUTE
INNER JOIN amzl_mapping MAP ON 1=1
    AND ROUTE.region_id = 1
    AND ROUTE."date" >= '2025-03-01'::TIMESTAMP
    AND MAP.node = ROUTE.station_code
WHERE 1=1
GROUP BY "date", station_code;
```

### Weekly & Monthly Aggregations with Volatility Metrics

```sql
-- Monthly Aggregation with Volatility and Inducted Volume Filter
WITH monthly_data AS (
    SELECT
        DATE_PART('year', date) AS year,
        DATE_PART('month', date) AS month,
        DATE_PART('week', date) AS week_of_year,
        station_code,
        SUM(is_inducted) AS inducted_volume,
        SUM(is_inducted_not_planned) AS inducted_not_planned,
        SUM(is_inducted) AS inducted,
        SUM(is_planned) AS planned
    FROM temp_data
    GROUP BY year, month, week_of_year, station_code
)
SELECT
    year,
    month,
    week_of_year,
    station_code,
    SUM(inducted_volume) AS inducted_volume,
    SUM(inducted_not_planned) AS inducted_not_planned,
    SUM(inducted) AS inducted,
    SUM(planned) AS planned,
    CASE
        WHEN SUM(inducted) = 0 THEN 0
        ELSE ROUND(100.0 * SUM(inducted_not_planned) / SUM(inducted), 2)
    END AS pct_inducted_not_planned,
    STDDEV_POP(CASE
        WHEN inducted = 0 THEN 0
        ELSE 100.0 * inducted_not_planned / inducted
    END) AS volatility_inducted_not_planned
FROM monthly_data
WHERE inducted_volume >= 1000 -- Adjust this threshold as needed
GROUP BY year, month, week_of_year, station_code
ORDER BY year, month, week_of_year, station_code;
```

## Understanding Standard Deviation (STDDEV_POP)

The STDDEV_POP function calculates the population standard deviation, which measures how spread out the values are from the mean. In the context of no-cycle analysis:

- A value of 0 indicates no variability (all days within the aggregation period had identical percentages)
- Higher values indicate more variability in the no-cycle percentages within the period
- If you're getting all zeros for volatility_inducted_not_planned, this could be because:
  1. There's only one data point in the group (so no variance is possible)
  2. All data points have exactly the same no-cycle percentage
  3. The inducted volumes are very low, causing rounded percentages to be identical

For more meaningful volatility metrics:
- Ensure adequate sample sizes (multiple days per aggregation period)
- Consider using VARIANCE or STDDEV_SAMP as alternatives
- Filter out periods with very low inducted volumes

## Location Mapping Table

Always use the location mapping table to ensure proper timezone handling:

```sql
CREATE TEMP TABLE amzl_mapping AS (
    SELECT
        location_id AS node,
        timezone,
        region,
        country,
        GETDATE() AS run_time_utc,
        CAST(CAST(GETDATE() AS DATE) AS TIMESTAMP) AS day_start_utc,
        CONVERT_TIMEZONE('UTC', timezone, GETDATE()) AS run_time_local,
        CONVERT_TIMEZONE('UTC', timezone, CAST(CAST(GETDATE() AS DATE) AS TIMESTAMP)) AS local_offset_utc
    FROM
        "amzlanalytics"."perfectmile"."d_perfectmile_node_mapping_mdm"
    WHERE
        1=1
        AND location_status = 'A'
        AND country IN('US', 'CA')
        -- Uncomment to filter by specific regions if needed
        -- AND UPPER(region) IN('ROCKIES', 'UPSTATE NY') 
        AND (location_type = 'DS' 
            OR location_id IN ('MCO5', 'ZYG1', 'ZYN9', 'XVV2', 'XVV3', 'XYT6', 'XLC1', 'XVC1', 'XNK2')
        )
);
```

## Best Practices for No-Cycle Analysis

1. **Filter by Minimum Volume**: Always include volume thresholds to prevent skewed results from low-volume periods
2. **Use Time-Based Aggregations**: Analyze patterns by day, week, and month to identify trends and anomalies
3. **Calculate Volatility Metrics**: Use standard deviation to understand stability of no-cycle percentages
4. **Apply Timezone Adjustments**: Use location mapping to ensure proper timezone handling
5. **Create Base Tables First**: Use temporary tables with base calculations for efficient multiple aggregations
6. **Compare Across Dimensions**: Analyze by station, region, originating node, and time periods

## Extended Analysis Techniques

### Separating Date Components
When analyzing time-based patterns, separate date components for more flexible aggregation:
```sql
SELECT
    DATE_PART('year', date) AS year,
    DATE_PART('month', date) AS month,
    DATE_PART('week', date) AS week_of_year,
    station_code,
    -- Metrics
FROM temp_data
GROUP BY year, month, week_of_year, station_code
```

### Volume-Weighted Analysis
Weight station performance by volume to ensure high-volume stations have appropriate influence:
```sql
SELECT
    region,
    SUM(is_inducted * pct_inducted_not_planned) / SUM(is_inducted) AS weighted_no_cycle_pct
FROM temp_data
GROUP BY region
```

### Recommended Python Analysis Tools
For visualizing and analyzing no-cycle data:
- Pandas for data manipulation
- Matplotlib/Seaborn for visualizations
- Use the provided `container_analysis.ipynb` notebook as a starting point

## File Locations

- Sample SQL queries: `/home/admsia/shipment_timeline/no_cycle/container_planning_sample_queries.sql`
- Container planning analysis: `/home/admsia/shipment_timeline/container_planning/analysis/`
- Jupyter notebook for visualization: `/home/admsia/shipment_timeline/container_planning/analysis/container_analysis.ipynb`
- Excel data reading script: `/home/admsia/view_excel.py`

## Data Sources

1. **Core Container Planning Data**:
   - `amzlanalytics.amzlcore.amzl_routing2_container_pkg_na` - Container planning and execution metrics
   
2. **Location Reference Data**:
   - `amzlanalytics.perfectmile.d_perfectmile_node_mapping_mdm` - Node location and timezone mapping

3. **Parquet Files**:
   - `s3://altdatasetexfil/claudecloud/routing2_container_snip/date=YYYY-MM-DD 00:00:00/station_code=XXX/`
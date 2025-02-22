-- Views 

-- TODO: UPLOAD THIS DATASET ATTACHED TO BIGQUERY. 

-- now lets run a simple select statment from which we will use to create our views 
-- this is an airline customr booking dataset
-- we wont go into the detail of it but come up with scenarios 
-- that we will use to create views 

SELECT * FROM `youtubescripts-425810.youtubebigquery.customerbooking`; 


-- LOGICAL VIEWS
-- A logical (or standard) view is essentially a saved SQL query, similar to a "virtual table."

CREATE VIEW `youtubescripts-425810.youtubebigquery.logical_viewtesting`  AS
SELECT 
    route,
    COUNT(num_passengers) AS total_passengers,
    AVG(length_of_stay) AS avg_length_of_stay,
    SUM(wants_extra_baggage) AS total_baggage_requests
FROM `youtubescripts-425810.youtubebigquery.customerbooking`
GROUP BY route;


-- LIMITATIONS 
-- Views are read-only meaning you cannot run queries that insert, update or delete data.
-- You cannot reference parameters in views.


-- MATERIALISED VIEWS 
-- they store the precomputed results of a query in BigQuery storage.
-- BigQuery refreshes these views incrementally by only recomputing changes since the last refresh. 

CREATE MATERIALIZED VIEW `youtubescripts-425810.youtubebigquery.materializedtest` AS
SELECT 
    route,
    COUNT(num_passengers) AS total_passengers,
    AVG(length_of_stay) AS avg_length_of_stay,
    SUM(wants_extra_baggage) AS total_baggage_requests
FROM `youtubescripts-425810.youtubebigquery.customerbooking`
GROUP BY route;

-- Use cases
-- High-Frequency Reporting: Dashboards or reports that hit the same aggregated metrics repeatedly.
-- Large Tables,Repetitive Queries: Scenarios where scanning the entire underlying table every time would be cost-prohibitive or too slow.

-- Limitations
-- SQL Features like Windows functions ie ROW_NUMBER() OR RANK() (depends on the entire dataset not just new or changed rows)
-- non deterministic functions like CURRENT_TIMESTAMP(), generates different results everytime a query is run 
-- Joins like Right Outer Joins are not supported in materialized views.

-- Example 1: Using a window function (ROW_NUMBER) which is not allowed
CREATE MATERIALIZED VIEW `youtubescripts-425810.youtubebigquery.materializedtestlimitation` AS
SELECT 
  route,
  flight_day,
  num_passengers,
  ROW_NUMBER() OVER (PARTITION BY route ORDER BY flight_day) AS row_num
FROM `youtubescripts-425810.youtubebigquery.customerbooking`;


-- NON INCREMENTAL MATERIALISED VIEWS 
-- this is materialized view that cannot leverage incremental refresh and therefore must be fully or largely recomputed on each refresh.
-- because Certain query patterns or transformations break the rules for incremental refresh. For example, some complex joins, window functions,
-- or unsupported aggregation types may prevent partial updates.

CREATE MATERIALIZED VIEW `youtubescripts-425810.youtubebigquery.non_incremental_viewtesttest` 
OPTIONS (
  enable_refresh = true, 
  refresh_interval_minutes = 60, 
  max_staleness = INTERVAL "4" HOUR, 
  allow_non_incremental_definition = true
) AS
SELECT 
  route,
  flight_day,
  num_passengers,
  ROW_NUMBER() OVER (PARTITION BY route ORDER BY flight_day) AS row_num
FROM `youtubescripts-425810.youtubebigquery.customerbooking`;


-- Use cases
-- When you need to precompute data that can't be expressed in an "incremental-friendly" way (e.g., certain window functions or nested subqueries).
-- Regulatory or Validation Requirements: Some teams prefer recalculating everything to ensure absolute consistency and correctness on certain schedules.
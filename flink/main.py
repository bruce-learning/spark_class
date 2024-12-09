import time
from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes

env_settings = EnvironmentSettings.new_instance().in_batch_mode().build()
t_env = TableEnvironment.create(env_settings)

t_env.execute_sql("""
    CREATE TEMPORARY TABLE source_table (
        user_id STRING,
        item_id STRING,
        behavior_type INT,
        item_category STRING,
        `time` STRING,
	    `date` STRING,
	    `hour` INT
    ) WITH (
        'connector' = 'filesystem',
        'path' = 'user_action.csv',
        'format' = 'csv',
        'csv.field-delimiter' = ',',
        'csv.ignore-parse-errors' = 'true'
    )
""")

tbl = t_env.from_path("source_table")
t_env.create_temporary_view("tmp_view", tbl)


def run_query(name, sql, fields, print_result=True):
    start_time = time.time()
    result_table = t_env.execute_sql(sql)
    rows = result_table.collect()
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f'query {name} executed in {elapsed_time:.4f} seconds')
    f = open(f'{name}.txt', 'w')
    if print_result:
        for row in rows:
            f.write(', '.join(f'{field}: {row[idx]}' for idx, field in enumerate(fields)) + '\n')


run_query('counting', """
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT user_id) AS distinct_users,
    COUNT(DISTINCT item_id) AS distinct_items,
    COUNT(DISTINCT item_category) AS distinct_categories
FROM tmp_view
""", ['entry_count', 'user_count', 'product_count', 'product_category_count'])

run_query('pv-uv-by-date', """
SELECT
    `date`,
    COUNT(*) AS pv,
    COUNT(DISTINCT user_id) AS uv
FROM tmp_view
GROUP BY `date`
ORDER BY `date`;
""", ['date', 'pv', 'uv']
)

run_query('pv-uv-by-hour', """
SELECT
    `hour`,
    COUNT(*) AS pv,
    COUNT(DISTINCT user_id) AS uv
FROM tmp_view
GROUP BY `hour`
ORDER BY `hour`;
""", ['hour', 'pv', 'uv']
)

run_query('pv-uv-by-hour-in-2014-12-12', """
SELECT
    `hour`,
    COUNT(*) AS pv,
    COUNT(DISTINCT user_id) AS uv
FROM tmp_view
WHERE `date` = '2014-12-12'
GROUP BY `hour`
ORDER BY `hour`;
""", ['hour', 'pv', 'uv'])

run_query('behavior-pv-by-hour', """
SELECT
    `behavior_type`,
    `hour`,
    COUNT(*) AS pv_behavior
FROM tmp_view
GROUP BY `behavior_type`, `hour`
ORDER BY `behavior_type`, `hour`;
""", ['behavior_type', 'hour', 'pv'])

run_query('conversion-rate', """
SELECT
    `behavior_type`,
    COUNT(*) AS behavior_count
FROM tmp_view
GROUP BY `behavior_type`
ORDER BY `behavior_type`;
""", ['behavior_type', 'count'])

run_query('conversion-rate-2014-12-12', """
SELECT
    `behavior_type`,
    COUNT(*) AS behavior_count
FROM tmp_view
WHERE `date` = '2014-12-12'
GROUP BY `behavior_type`
ORDER BY `behavior_type`;
""", ['behavior_type', 'count'])

run_query('buy-count', """
SELECT
    `user_id`,
    COUNT(*) AS buy_count
FROM tmp_view
WHERE `behavior_type` = 4
GROUP BY `user_id`
ORDER BY `user_id`;
""", ['user_id', 'buy_count'])


run_query('arpu', """
WITH daily_metrics AS (
    SELECT
        `date`,
        COUNT(CASE WHEN behavior_type = 4 THEN 1 END) AS total_consumption_count,
        COUNT(DISTINCT user_id) AS active_user_count
    FROM
        source_table
    GROUP BY
        `date`
),
daily_ratio AS (
    SELECT
        `date`,
        total_consumption_count,
        active_user_count,
        CASE 
            WHEN active_user_count > 0 THEN total_consumption_count * 1.0 / active_user_count
            ELSE 0
        END AS consumption_per_user_ratio
    FROM
        daily_metrics
)
SELECT
    `date`,
    total_consumption_count,
    active_user_count,
    consumption_per_user_ratio
FROM
    daily_ratio
ORDER BY
    `date`;
""", ['date', 'total_consumption_count', 'active_user_count', 'consumption_per_user_ratio'])


run_query('arrpu', """
WITH daily_metrics AS (
    SELECT
        `date`,
        COUNT(DISTINCT CASE WHEN behavior_type = 4 THEN user_id END) AS paying_user_count,
        COUNT(DISTINCT user_id) AS active_user_count
    FROM
        source_table
    GROUP BY
        `date`
),
daily_ratio AS (
    SELECT
        `date`,
        paying_user_count,
        active_user_count,
        CASE 
            WHEN active_user_count > 0 THEN paying_user_count * 1.0 / active_user_count
            ELSE 0
        END AS paying_to_active_ratio
    FROM
        daily_metrics
)
SELECT
    `date`,
    paying_user_count,
    active_user_count,
    paying_to_active_ratio
FROM
    daily_ratio
ORDER BY
    `date`;
""", ['date', 'paying_user_count', 'active_user_count', 'paying_to_active_ratio'])


run_query('rebuy-average-date', """
WITH purchase_data AS (
    SELECT
        user_id,
        item_id,
        TO_DATE(`date`) AS purchase_date
    FROM
        source_table
    WHERE
        behavior_type = 4
),
user_item_diff AS (
    SELECT
        user_id,
        item_id,
        purchase_date,
        LEAD(purchase_date) OVER (PARTITION BY user_id, item_id ORDER BY purchase_date) AS next_purchase_date
    FROM
        purchase_data
),
purchase_diff AS (
    SELECT
        user_id,
        item_id,
        TIMESTAMPDIFF(DAY, purchase_date, next_purchase_date) AS diff_days
    FROM
        user_item_diff
    WHERE
        next_purchase_date IS NOT NULL
),
user_avg_repurchase AS (
    SELECT
        user_id,
        AVG(diff_days) AS avg_repurchase_days
    FROM
        purchase_diff
    GROUP BY
        user_id
)
SELECT
    user_id,
    avg_repurchase_days
FROM
    user_avg_repurchase;
""", ['user_id', 'average_rebuy_date'])

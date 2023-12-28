-- テーブルの作成
CREATE TABLE IF NOT EXISTS `mart.storage_analysis` (
    project_id STRING,
    dataset_id STRING,
    storage_type STRING,
    logic_active FLOAT64,
    logic_long_term FLOAT64,
    comp_active FLOAT64,
    comp_long_term FLOAT64,
    comp_time_travel FLOAT64,
    comp_fail_safe FLOAT64,
    diff_logic_active FLOAT64,
    diff_logic_long_term FLOAT64,
    diff_comp_active FLOAT64,
    diff_comp_long_term FLOAT64,
    diff_comp_time_travel FLOAT64,
    diff_comp_fail_safe FLOAT64,
    date DATE,
    year_month STRING,
    year STRING
) PARTITION BY date;

CREATE TEMP TABLE current_storage_usages AS
SELECT
    project_id,
    table_schema AS dataset_id,
    SUM((active_logical_bytes / POW(1024, 3))) AS active_logical_gigabytes,
    SUM((long_term_logical_bytes / POW(1024, 3))) AS long_term_logical_gigabytes,
    SUM(
        (
            (
                active_physical_bytes - (
                    time_travel_physical_bytes + fail_safe_physical_bytes
                )
            ) / POW(1024, 3)
        )
    ) AS active_compressed_gigabytes,
    SUM((long_term_physical_bytes / POW(1024, 3))) AS long_term_compressed_gigabytes,
    SUM((time_travel_physical_bytes / POW(1024, 3))) AS time_travel_compressed_gigabytes,
    SUM((fail_safe_physical_bytes / POW(1024, 3))) AS fail_safe_compressed_gigabytes
FROM
    `region-asia-northeast1.INFORMATION_SCHEMA.TABLE_STORAGE_BY_ORGANIZATION`
GROUP BY
    1,
    2;

CREATE TEMP TABLE storage_billing_model(
    project_id STRING,
    dataset_id STRING,
    storage_type STRING
);

FOR current_storage_usage IN (
    SELECT
        DISTINCT project_id
    FROM
        current_storage_usages
) DO BEGIN EXECUTE IMMEDIATE format(
    """
    INSERT INTO storage_billing_model(project_id, dataset_id, storage_type)
    SELECT
      catalog_name AS project_id
      ,schema_name AS dataset_id
      ,option_value AS storage_type
    FROM
      `%s.region-asia-northeast1.INFORMATION_SCHEMA.SCHEMATA_OPTIONS`
    WHERE
      option_name = 'storage_billing_model'
  """,
    current_storage_usage.project_id
);

EXCEPTION
WHEN ERROR THEN
END;

END FOR;

INSERT INTO
    `mart.storage_analysis`(
        project_id,
        dataset_id,
        storage_type,
        logic_active,
        logic_long_term,
        comp_active,
        comp_long_term,
        comp_time_travel,
        comp_fail_safe,
        diff_logic_active,
        diff_logic_long_term,
        diff_comp_active,
        diff_comp_long_term,
        diff_comp_time_travel,
        diff_comp_fail_safe,
        date,
        year_month,
        year
    ) WITH storage_usages_one_day_ago AS (
        SELECT
            project_id,
            dataset_id,
            logic_active,
            logic_long_term,
            comp_active,
            comp_long_term,
            comp_time_travel,
            comp_fail_safe
        FROM
            `mart.storage_analysis`
        WHERE
            date = DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 1 DAY)
    )
SELECT
    current_storage_usages.project_id,
    current_storage_usages.dataset_id,
    REPLACE(
        ifnull(storage_type, 'logical'),
        'PHYSICAL',
        'compressed'
    ),
    current_storage_usages.active_logical_gigabytes AS logic_active,
    current_storage_usages.long_term_logical_gigabytes,
    current_storage_usages.active_compressed_gigabytes,
    current_storage_usages.long_term_compressed_gigabytes,
    current_storage_usages.time_travel_compressed_gigabytes,
    current_storage_usages.fail_safe_compressed_gigabytes,
    current_storage_usages.active_logical_gigabytes - storage_usages_one_day_ago.logic_active,
    current_storage_usages.long_term_logical_gigabytes - storage_usages_one_day_ago.logic_long_term,
    current_storage_usages.active_compressed_gigabytes - storage_usages_one_day_ago.comp_active,
    current_storage_usages.long_term_compressed_gigabytes - storage_usages_one_day_ago.comp_long_term,
    current_storage_usages.time_travel_compressed_gigabytes - storage_usages_one_day_ago.comp_time_travel,
    current_storage_usages.fail_safe_compressed_gigabytes - storage_usages_one_day_ago.comp_fail_safe,
    CURRENT_DATE('Asia/Tokyo') AS created_jst_date,
    SUBSTR(CAST(CURRENT_DATE('Asia/Tokyo') AS STRING), 0, 7),
    SUBSTR(CAST(CURRENT_DATE('Asia/Tokyo') AS STRING), 0, 4)
FROM
    current_storage_usages
    LEFT JOIN storage_billing_model ON current_storage_usages.project_id = storage_billing_model.project_id
    AND current_storage_usages.dataset_id = storage_billing_model.dataset_id
    LEFT JOIN storage_usages_one_day_ago ON current_storage_usages.project_id = storage_usages_one_day_ago.project_id
    AND current_storage_usages.dataset_id = storage_usages_one_day_ago.dataset_id
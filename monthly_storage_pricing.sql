CREATE TABLE IF NOT EXISTS `mart.monthly_storage_pricing` (
    project_id STRING,
    logic_active_price FLOAT64,
    logic_long_term_price FLOAT64,
    comp_active_price FLOAT64,
    comp_long_term_price FLOAT64,
    logic_total_price FLOAT64,
    comp_total_price FLOAT64,
    active_total_price FLOAT64,
    long_term_total_price FLOAT64,
    total_price FLOAT64,
    year STRING,
    year_month STRING,
    year_month_beginning DATE
) PARTITION BY year_month_beginning;

CREATE TEMP FUNCTION storage_price(storage_class STRING) AS (
    CASE
        storage_class
        WHEN "comp_active" THEN (
            SELECT
                active_compressed_pricing
            FROM
                `bigquery_pricing.storage`
            WHERE
                application_start_date = (
                    SELECT
                        MAX(application_start_date)
                    FROM
                        `bigquery_pricing.storage`
                )
                AND region = 'asia-northeast1'
        )
        WHEN "comp_long_term" THEN (
            SELECT
                long_term_compressed_pricing
            FROM
                `bigquery_pricing.storage`
            WHERE
                application_start_date = (
                    SELECT
                        MAX(application_start_date)
                    FROM
                        `bigquery_pricing.storage`
                )
                AND region = 'asia-northeast1'
        )
        WHEN "logic_active" THEN (
            SELECT
                active_logical_pricing
            FROM
                `bigquery_pricing.storage`
            WHERE
                application_start_date = (
                    SELECT
                        MAX(application_start_date)
                    FROM
                        `bigquery_pricing.storage`
                )
                AND region = 'asia-northeast1'
        )
        WHEN "logic_long_term" THEN (
            SELECT
                long_term_logical_pricing
            FROM
                `bigquery_pricing.storage`
            WHERE
                application_start_date = (
                    SELECT
                        MAX(application_start_date)
                    FROM
                        `bigquery_pricing.storage`
                )
                AND region = 'asia-northeast1'
        )
    END
);

MERGE `mart.monthly_storage_pricing` AS target USING (
    WITH storage_usage AS (
        SELECT
            project_id,
            -- 10GBの無料枠は請求先アカウントごとの無料枠なので、料金マートでは考慮しない
            AVG(logic_active) AS monthly_logic_active,
            AVG(logic_long_term) AS monthly_logic_long_term,
            AVG(comp_active) AS monthly_comp_active,
            AVG(comp_long_term) AS monthly_comp_long_term,
            SUBSTR(CAST(date AS STRING), 1, 4) AS year,
            SUBSTR(CAST(date AS STRING), 6, 2) AS month
        FROM
            `mart.daily_storage_usage`
        WHERE
            SUBSTR(CAST(date AS STRING), 1, 4) = SUBSTR(CAST(CURRENT_DATE('Asia/Tokyo') AS STRING), 1, 4)
            AND SUBSTR(CAST(date AS STRING), 6, 2) = SUBSTR(CAST(CURRENT_DATE('Asia/Tokyo') AS STRING), 6, 2)
        GROUP BY
            project_id,
            SUBSTR(CAST(date AS STRING), 1, 4),
            SUBSTR(CAST(date AS STRING), 6, 2)
    )
    SELECT
        project_id,
        monthly_logic_active * storage_price("logic_active") AS logic_active_price,
        monthly_logic_long_term * storage_price("logic_long_term") AS logic_long_term_price,
        monthly_comp_active * storage_price("comp_active") AS comp_active_price,
        monthly_comp_long_term * storage_price("comp_long_term") AS comp_long_term_price,
        monthly_logic_active * storage_price("logic_active") + monthly_logic_long_term * storage_price("logic_long_term") AS logic_total_price,
        monthly_comp_active * storage_price("comp_active") + monthly_comp_long_term * storage_price("comp_long_term") AS comp_total_price,
        monthly_logic_active * storage_price("logic_active") + monthly_comp_active * storage_price("comp_active") AS active_total_price,
        monthly_logic_long_term * storage_price("logic_long_term") + monthly_comp_long_term * storage_price("comp_long_term") AS long_term_total_price,
        monthly_logic_active * storage_price("logic_active") + monthly_logic_long_term * storage_price("logic_long_term") + monthly_comp_active * storage_price("comp_active") + monthly_comp_long_term * storage_price("comp_long_term") AS total_price,
        year,
        year || '-' || month AS year_month,
        DATE(year || '-' || month || '-' || '01') AS year_month_beginning
    FROM
        storage_usage
) AS source ON target.project_id = source.project_id
AND target.year_month = source.year_month
WHEN MATCHED THEN
UPDATE
SET
    target.logic_active_price = source.logic_active_price,
    target.logic_long_term_price = source.logic_long_term_price,
    target.comp_active_price = source.comp_active_price,
    target.comp_long_term_price = source.comp_long_term_price,
    target.logic_total_price = source.logic_total_price,
    target.comp_total_price = source.comp_total_price,
    target.active_total_price = source.active_total_price,
    target.long_term_total_price = source.long_term_total_price,
    target.total_price = source.total_price
    WHEN NOT MATCHED THEN
INSERT
    (
        project_id,
        logic_active_price,
        logic_long_term_price,
        comp_active_price,
        comp_long_term_price,
        logic_total_price,
        comp_total_price,
        active_total_price,
        long_term_total_price,
        total_price,
        year,
        year_month,
        year_month_beginning
    )
VALUES
    (
        source.project_id,
        source.logic_active_price,
        source.logic_long_term_price,
        source.comp_active_price,
        source.comp_long_term_price,
        source.logic_total_price,
        source.comp_total_price,
        source.active_total_price,
        source.long_term_total_price,
        source.total_price,
        source.year,
        source.year_month,
        source.year_month_beginning
    )
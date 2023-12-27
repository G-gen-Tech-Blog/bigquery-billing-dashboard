-- テーブルの作成
CREATE TABLE IF NOT EXISTS `mart.daily_ondemand_compute_pricing` (
    project_id STRING,
    cost FLOAT64,
    year STRING,
    year_month STRING,
    date Date
) PARTITION BY date;

-- UDFの作成
CREATE TEMP FUNCTION compute_price(compute_class STRING) AS (
    (
        SELECT
            pay_as_you_go_pricing
        FROM
            `bigquery_pricing.compute`
        WHERE
            application_start_date = (
                SELECT
                    MAX(application_start_date)
                FROM
                    `bigquery_pricing.compute`
            )
            AND region = 'asia-northeast1'
            AND class = compute_class
    )
);

-- INFORMATION_SCHEMAから該当データを取得し、マートテーブルにマージ
MERGE `mart.daily_ondemand_compute_pricing` AS target USING (
    SELECT
        project_id,
        DATE(
            FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', creation_time, 'Asia/Tokyo')
        ) AS date,
        CAST(
            EXTRACT(
                YEAR
                FROM
                    DATE(
                        FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', creation_time, 'Asia/Tokyo')
                    )
            ) AS STRING
        ) AS year,
        EXTRACT(
            YEAR
            FROM
                DATE(
                    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', creation_time, 'Asia/Tokyo')
                )
        ) || '-' || LPAD(
            CAST(
                EXTRACT(
                    MONTH
                    FROM
                        DATE(
                            FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', creation_time, 'Asia/Tokyo')
                        )
                ) AS STRING
            ),
            2,
            '0'
        ) AS year_month,
        SUM(total_bytes_billed) / POW(1024, 4) * compute_price('on_demand') AS cost
    FROM
        `region-asia-northeast1.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION`
    WHERE
        DATE(
            DATE_TRUNC(
                DATE(
                    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', creation_time, 'Asia/Tokyo')
                ),
                MONTH
            )
        ) BETWEEN DATE_TRUNC(
            DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 1 MONTH),
            MONTH
        )
        AND CURRENT_DATE('Asia/Tokyo')
        AND reservation_id IS NULL
        AND statement_type != 'SCRIPT'
    GROUP BY
        1,
        2,
        3,
        4
) AS source ON target.project_id = source.project_id
AND target.date = source.date
WHEN MATCHED THEN
UPDATE
SET
    target.cost = source.cost
    WHEN NOT MATCHED THEN
INSERT
    (
        project_id,
        cost,
        year,
        year_month,
        date
    )
VALUES
    (
        source.project_id,
        source.cost,
        source.year,
        source.year_month,
        source.date
    )
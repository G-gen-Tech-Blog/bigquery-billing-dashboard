CREATE TABLE IF NOT EXISTS `mart.daily_editions_compute_pricing` (
    reservation_id STRING,
    date DATE,
    year STRING,
    year_month STRING,
    edition STRING,
    baseline_cost FLOAT64,
    cost FLOAT64
) PARTITION BY date;

CREATE TEMP FUNCTION compute_price(compute_class STRING) AS (
    CASE
        compute_class
        WHEN "STANDARD" THEN (
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
                AND class = 'standard'
        )
        WHEN "ENTERPRISE" THEN (
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
                AND class = 'enterprise'
        )
        WHEN "ENTERPRISE_PLUS" THEN (
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
                AND class = 'enterprise_plus'
        )
    END
);

CREATE TEMP TABLE res(
    reservation_id STRING,
    baseline INT64,
    period_start TIMESTAMP,
    edition STRING
);

CREATE TEMP TABLE job AS
SELECT
    reservation_id,
    period_start,
    period_slot_ms
FROM
    `region-asia-northeast1`.INFORMATION_SCHEMA.JOBS_TIMELINE_BY_ORGANIZATION
WHERE
    DATE(
        DATE_TRUNC(
            DATE(
                FORMAT_TIMESTAMP(
                    '%Y-%m-%d %H:%M:%S',
                    job_creation_time,
                    'Asia/Tokyo'
                )
            ),
            MONTH
        )
    ) BETWEEN DATE_SUB(
        DATE_TRUNC(CURRENT_DATE('Asia/Tokyo'), MONTH),
        INTERVAL 1 DAY
    )
    AND CURRENT_DATE('Asia/Tokyo')
    AND DATE(
        DATE_TRUNC(
            DATE(
                FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', period_start, 'Asia/Tokyo')
            ),
            MONTH
        )
    ) BETWEEN DATE_TRUNC(CURRENT_DATE('Asia/Tokyo'), MONTH)
    AND CURRENT_DATE('Asia/Tokyo')
    AND reservation_id IS NOT NULL
    AND reservation_id != 'default-pipeline'
    AND (
        statement_type != "SCRIPT"
        OR statement_type IS NULL
    );

-- reservation情報の取得
FOR job IN (
    SELECT
        DISTINCT reservation_id
    FROM
        job
) DO EXECUTE IMMEDIATE FORMAT(
    """
INSERT INTO res(reservation_id, baseline, period_start, edition)
  SELECT 
    t.reservation_id,
    t.slots_assigned,
    t.period_start,
    t2.edition
  FROM
    `%s.region-asia-northeast1.INFORMATION_SCHEMA.RESERVATION_TIMELINE` t
  LEFT JOIN
    `%s.region-asia-northeast1.INFORMATION_SCHEMA.RESERVATIONS` t2
    ON t.project_id = t2.project_id
    and t.project_number = t2.project_number
""",
    REPLACE(
        REGEXP_EXTRACT(job.reservation_id, r'^.*:'),
        ':',
        ''
    ),
    REPLACE(
        REGEXP_EXTRACT(job.reservation_id, r'^.*:'),
        ':',
        ''
    )
);

END FOR;

MERGE `mart.daily_editions_compute_pricing` AS target USING (
    WITH slot_per_minutes AS (
        SELECT
            res.reservation_id,
            res.period_start AS period_minute,
            baseline,
            edition,
            CASE
                WHEN baseline > 0 THEN CEIL(
                    SUM(((job.period_slot_ms) / 1000) - baseline) / 60 / 100
                ) * 100
                ELSE CEIL(SUM((job.period_slot_ms / 1000)) / 60 / 100) * 100
            END AS slot_sec_average_in_minute
        FROM
            job
            JOIN res ON TIMESTAMP_TRUNC(job.period_start, MINUTE) = res.period_start
            AND job.reservation_id = res.reservation_id
        GROUP BY
            1,
            2,
            3,
            4
    ),
    slot_hour AS (
        SELECT
            reservation_id,
            TIMESTAMP_TRUNC(period_minute, HOUR) AS period_hour,
            edition,
            baseline,
            CEIL(
                SUM(slot_sec_average_in_minute) / 3600 * 60 * 100
            ) / 100 AS slot_hour_optimistic
        FROM
            slot_per_minutes
        GROUP BY
            1,
            2,
            3,
            4
    )
    SELECT
        reservation_id,
        DATE(period_hour, 'Asia/Tokyo') AS date,
        CAST(
            EXTRACT(
                YEAR
                FROM
                    DATE(period_hour, 'Asia/Tokyo')
            ) AS STRING
        ) AS year,
        EXTRACT(
            YEAR
            FROM
                DATE(period_hour, 'Asia/Tokyo')
        ) || '-' || LPAD(
            CAST(
                EXTRACT(
                    MONTH
                    FROM
                        DATE(period_hour, 'Asia/Tokyo')
                ) AS STRING
            ),
            2,
            '0'
        ) AS year_month,
        edition,
        baseline * 24 * compute_price(edition) AS baseline_cost,
        SUM(slot_hour_optimistic) * compute_price(edition) AS cost
    FROM
        slot_hour
    GROUP BY
        1,
        2,
        3,
        4,
        5,
        6
) AS source ON target.reservation_id = source.reservation_id
AND target.date = source.date
WHEN MATCHED THEN
UPDATE
SET
    target.cost = source.cost,
    target.baseline_cost = source.baseline_cost
    WHEN NOT MATCHED THEN
INSERT
    (
        reservation_id,
        date,
        year,
        year_month,
        edition,
        baseline_cost,
        cost
    )
VALUES
    (
        source.reservation_id,
        source.date,
        source.year,
        source.year_month,
        source.edition,
        source.baseline_cost,
        source.cost
    )
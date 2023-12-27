-- テーブルの作成
CREATE TABLE IF NOT EXISTS `mart.slot_analysis` (
    project_id STRING,
    job_id STRING,
    account STRING,
    reservation_id STRING,
    cache BOOLEAN,
    slot_min FLOAT64,
    slot_max FLOAT64,
    job_start TIMESTAMP,
    job_end TIMESTAMP,
    job_time FLOAT64,
    query_type STRING,
    query STRING,
    date DATE,
    year_month STRING,
    year STRING
) PARTITION BY date;

CREATE TEMP TABLE jobs AS
SELECT
    project_id,
    reservation_id,
    job_id,
    user_email,
    cache_hit,
    start_time,
    end_time,
    TIMESTAMP_DIFF(end_time, start_time, MILLISECOND) / 1000 AS query_execution_seconds,
    statement_type
FROM
    `region-asia-northeast1.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION`
WHERE
    start_time BETWEEN TIMESTAMP(
        DATE_TRUNC(
            DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 1 DAY),
            DAY
        )
    )
    AND TIMESTAMP(
        TIMESTAMP_SUB(
            CURRENT_DATETIME('Asia/Tokyo'),
            INTERVAL 5 MINUTE
        )
    )
    AND state = 'DONE';

CREATE TEMP TABLE query(
    project_id STRING,
    job_id STRING,
    user_email STRING,
    statement STRING
);

FOR job IN (
    SELECT
        DISTINCT project_id
    FROM
        jobs
) DO EXECUTE IMMEDIATE format(
    """
    INSERT INTO query(project_id, job_id, user_email, statement)
    SELECT
      project_id
      ,job_id 
      ,user_email
      ,query
    FROM
      `%s.region-asia-northeast1.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
  """,
    job.project_id
);

END FOR;

CREATE TEMP TABLE slot(
    project_id STRING,
    job_id STRING,
    user_email STRING,
    reservation_id STRING,
    optimistic_slot_hour FLOAT64,
    pessmistic_slot_hour FLOAT64
);

INSERT INTO
    slot(
        project_id,
        job_id,
        user_email,
        reservation_id,
        optimistic_slot_hour,
        pessmistic_slot_hour
    ) WITH slot_sec_per_second AS (
        SELECT
            period_start,
            project_id,
            job_id,
            user_email,
            reservation_id,
            SUM(period_slot_ms) / 1000 AS slot_sec
        FROM
            `region-asia-northeast1.INFORMATION_SCHEMA.JOBS_TIMELINE_BY_ORGANIZATION`
        WHERE
            period_start BETWEEN TIMESTAMP(
                DATE_TRUNC(
                    DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 1 DAY),
                    DAY
                )
            )
            AND TIMESTAMP(
                TIMESTAMP_SUB(
                    CURRENT_DATETIME('Asia/Tokyo'),
                    INTERVAL 5 MINUTE
                )
            )
            AND state = 'DONE'
        GROUP BY
            1,
            2,
            3,
            4,
            5
    ),
    slot_sec_per_minute AS (
        SELECT
            TIMESTAMP_TRUNC(period_start, MINUTE) AS period_minute,
            project_id,
            job_id,
            user_email,
            reservation_id,
            CEIL(SUM(slot_sec) / 60 / 100) * 100 AS slot_sec_average_in_minute,
            CEIL(MAX(slot_sec) / 100) * 100 AS slot_sec_max_in_minute
        FROM
            slot_sec_per_second
        GROUP BY
            1,
            2,
            3,
            4,
            5
    )
SELECT
    project_id,
    job_id,
    user_email,
    reservation_id,
    CEIL(
        SUM(slot_sec_average_in_minute) / 3600 * 60 * 100
    ) / 100 AS optimistic_slot_hour,
    CEIL(SUM(slot_sec_max_in_minute) / 3600 * 60 * 100) / 100 AS pessmistic_slot_hour
FROM
    slot_sec_per_minute
GROUP BY
    1,
    2,
    3,
    4;

INSERT INTO
    `mart.slot_analysis` (
        project_id,
        job_id,
        account,
        reservation_id,
        cache,
        slot_min,
        slot_max,
        job_start,
        job_end,
        job_time,
        query_type,
        query,
        date,
        year_month,
        year
    )
SELECT
    jobs.project_id,
    jobs.job_id,
    jobs.user_email,
    jobs.reservation_id,
    jobs.cache_hit,
    slot.optimistic_slot_hour,
    slot.pessmistic_slot_hour,
    jobs.start_time,
    jobs.end_time,
    jobs.query_execution_seconds,
    jobs.statement_type,
    query.statement,
    CURRENT_DATE('Asia/Tokyo'),
    SUBSTR(CAST(CURRENT_DATE('Asia/Tokyo') AS STRING), 0, 7),
    SUBSTR(CAST(CURRENT_DATE('Asia/Tokyo') AS STRING), 0, 4)
FROM
    jobs
    LEFT JOIN query ON jobs.project_id = query.project_id
    AND jobs.job_id = query.job_id
    AND jobs.user_email = query.user_email
    LEFT JOIN slot ON jobs.project_id = slot.project_id
    AND jobs.job_id = slot.job_id
    AND jobs.user_email = slot.user_email
    AND jobs.reservation_id = slot.reservation_id
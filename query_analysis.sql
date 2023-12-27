-- テーブルの作成
CREATE TABLE IF NOT EXISTS `mart.query_analysis` (
    project_id STRING,
    job_id STRING,
    account STRING,
    query STRING,
    query_size FLOAT64,
    cache BOOLEAN,
    query_start TIMESTAMP,
    query_end TIMESTAMP,
    query_time FLOAT64,
    query_type STRING,
    date DATE,
    year_month STRING,
    year STRING
) PARTITION BY date;

CREATE TEMP TABLE jobs AS
SELECT
    project_id,
    job_id,
    user_email,
    cache_hit,
    start_time,
    end_time,
    TIMESTAMP_DIFF(end_time, start_time, MILLISECOND) / 1000 AS query_execution_seconds,
    statement_type,
    SUM(total_bytes_billed) / POW(1024, 4) AS total_terabytes_billed
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
    AND state = 'DONE'
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8;

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

INSERT INTO
    `mart.query_analysis` (
        project_id,
        job_id,
        account,
        query,
        query_size,
        cache,
        query_start,
        query_end,
        query_time,
        query_type,
        date,
        year_month,
        year
    )
SELECT
    jobs.project_id,
    jobs.job_id,
    jobs.user_email,
    query.statement,
    jobs.total_terabytes_billed,
    jobs.cache_hit,
    jobs.start_time,
    jobs.end_time,
    jobs.query_execution_seconds,
    jobs.statement_type,
    CURRENT_DATE('Asia/Tokyo') AS created_jst_date,
    SUBSTR(CAST(CURRENT_DATE('Asia/Tokyo') AS STRING), 0, 7),
    SUBSTR(CAST(CURRENT_DATE('Asia/Tokyo') AS STRING), 0, 4)
FROM
    jobs
    LEFT JOIN query ON jobs.project_id = query.project_id
    AND jobs.job_id = query.job_id
    AND jobs.user_email = query.user_email
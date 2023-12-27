-- テーブルの作成
CREATE TABLE IF NOT EXISTS `mart.daily_storage_usage` (
    project_id STRING,
    logic_active FLOAT64,
    logic_long_term FLOAT64,
    comp_active FLOAT64,
    comp_long_term FLOAT64,
    date DATE
) PARTITION BY date;

CREATE TEMP TABLE current_storage_usages AS
SELECT
    project_id,
    table_schema AS dataset_id,
    table_name AS table_id,
    active_logical_bytes,
    long_term_logical_bytes,
    active_physical_bytes,
    long_term_physical_bytes
FROM
    `region-asia-northeast1.INFORMATION_SCHEMA.TABLE_STORAGE_BY_ORGANIZATION`;

-- ストレージクラスを取得
-- 権限不足等でクエリできないテーブルは例外処理してスキップしている
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
    `mart.daily_storage_pricing`(
        project_id,
        logic_active,
        logic_long_term,
        comp_active,
        comp_long_term,
        date
    ) WITH storage_usage AS (
        -- storage_typeがnullになるレコードはlogicalとして分類
        SELECT
            current_storage_usages.project_id,
            current_storage_usages.dataset_id,
            table_id,
            CASE
                WHEN storage_type is null then active_logical_bytes
                ELSE 0
            END AS active_logical_bytes,
            CASE
                WHEN storage_type is null then long_term_logical_bytes
                ELSE 0
            END AS long_term_logical_bytes,
            CASE
                WHEN storage_type = 'PHYSICAL' then active_physical_bytes
                ELSE 0
            END AS active_physical_bytes,
            CASE
                WHEN storage_type = 'PHYSICAL' then long_term_physical_bytes
                ELSE 0
            END AS long_term_physical_bytes
        FROM
            current_storage_usages
            LEFT JOIN storage_billing_model ON current_storage_usages.project_id = storage_billing_model.project_id
            AND current_storage_usages.dataset_id = storage_billing_model.dataset_id
    )
SELECT
    project_id,
    SUM(active_logical_bytes) / POW(1024, 3),
    SUM(long_term_logical_bytes) / POW(1024, 3),
    SUM(active_physical_bytes) / POW(1024, 3),
    SUM(long_term_physical_bytes) / POW(1024, 3),
    CURRENT_DATE('Asia/Tokyo')
FROM
    storage_usage
GROUP BY
    1
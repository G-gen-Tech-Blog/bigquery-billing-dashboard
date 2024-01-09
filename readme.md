# 利用手順
## 1. SQL クエリの入手
- [GitHub](https://github.com/G-gen-Co-Ltd/da-solution-dashboard)
## 2. マートの作成
- GitHub から入手した SQLをベースに、スケジュールドクエリを作成する (参考情報: [クエリのスケジューリング](https://cloud.google.com/bigquery/docs/scheduling-queries?hl=ja))  
※ クエリ発行タイミングは、利用者様で設定（頻度が多い場合、コンピューティングの料金が上がるため注意が必要です）
## 3. 料金単価テーブルの作成
### 3.1 現在の料金単価を確認
以下の公式ドキュメント群から、asia-northeast1の料金単価を確認
- コンピュート料金単価
  - [オンデマンドクエリ料金単価](https://cloud.google.com/bigquery/pricing#:~:text=using%20Reservations.-,On%2Ddemand%20compute%20pricing,-By%20default%2C%20queries)
  - [Standard Edition 料金単価](https://cloud.google.com/bigquery/pricing#:~:text=of%20the%20period.-,Standard%20Edition,-The%20following%20table)
  - [Enterprise Edition 料金単価](https://cloud.google.com/bigquery/pricing#:~:text=1%20minute%20minimum-,Enterprise%20Edition,-The%20following%20table)
  - [Enterprise Plus Edition 料金単価](https://cloud.google.com/bigquery/pricing#:~:text=for%203%20years-,Enterprise%20Plus%20Edition,-The%20following%20table)
- ストレージ料金単価
  - [ストレージ料金単価](https://cloud.google.com/bigquery/pricing#:~:text=Platform%20SKUs%20apply.-,Storage%20pricing,-Storage%20pricing%20is)
### 3.2 テーブルを作成
- [3.1](#31-現在の料金単価を確認)で確認した料金単価になるように、以下のSQL文を書き換えてから実行
```sql
-- コンピュート料金単価
CREATE TABLE IF NOT EXISTS `bigquery_pricing.compute` (
    class STRING,
    pay_as_you_go_pricing FLOAT64,
    region STRING,
    application_start_date DATE
);
-- xxxxの部分を料金単価に、yyyy-mm-ddの部分を確認した日付に書き換える
INSERT `bigquery_pricing.compute`
(
    class,
    pay_as_you_go_pricing,
    region,
    application_start_date
)
VALUES('on_demand', xxxx, 'asia-northeast1', DATE('yyyy-mm-dd')),
('standard', xxxx, 'asia-northeast1', DATE('yyyy-mm-dd')),
('enterprise', xxxx, 'asia-northeast1', DATE('yyyy-mm-dd')),
('enterprise_plus', xxxx, 'asia-northeast1', DATE('yyyy-mm-dd'));
```
```sql
-- ストレージ料金単価
CREATE TABLE IF NOT EXISTS `bigquery_pricing.storage` (
    active_logical_pricing FLOAT64,
    long_term_logical_pricing FLOAT64,
    active_compressed_pricing FLOAT64,
    long_term_compressed_pricing FLOAT64,
    region STRING,
    application_start_date DATE
);
-- wwww, xxxx, yyyy, zzzzの部分をActive logical storage, Long-term logical storage, Active physical storage, Long-term physical storageの料金単価に
-- yyyy-mm-ddの部分を確認した日付に書き換える
INSERT `bigquery_pricing.storage`
(
    active_logical_pricing,
    long_term_logical_pricing,
    active_compressed_pricing,
    long_term_compressed_pricing,
    region,
    application_start_date
)
VALUES(wwww, xxxx, yyyy, zzzz, 'asia-northeast1', DATE('yyyy-mm-dd'));
```
※ BigQuery の料金単価が更新された場合、コンピュート、ストレージ料金単価テーブルに新たに行を追加する
## 4. ダッシュボードの作成
- [テンプレートURL](https://lookerstudio.google.com/u/0/reporting/ac95599a-da77-42f0-8c17-f65ca9ee94d5/preview)からダッシュボードを作成する
- データソースは [2.](#2-マートの作成) で作成したマートを参照する
- 適宜グラフなどを調整する
# Looker Studioカラム説明
## daily_editions_compute_pricing
| No  | 列名           | 説明                                                    |
| --- | -------------- | ------------------------------------------------------- |
| 1   | reservation_id | BigQuery Editionsの予約名                               |
| 2   | edition        | 使用したBigQuery Editionsの名前                         |
| 3   | baseline_cost  | ベースラインに設定したスロットにかかる料金 (単位: $/日) |
| 4   | cost           | Autoscalingで確保したスロットにかかる料金 (単位: $/日)  |
| 5   | date           | 使用日                                                  |
| 6   | year_month     | 使用日-年月                                             |
| 7   | year           | 使用日-年                                               |
## daily_ondemand_compute_pricing
| No  | 列名       | 説明                         |
| --- | ---------- | ---------------------------- |
| 1   | project_id | Google CloudのプロジェクトID |
| 2   | cost       | スキャン料金 (単位: $/日)    |
| 3   | date       | 使用日                       |
| 4   | year_month | 使用日-年月                  |
| 5   | year       | 使用日-年                    |
## monthly_storage_pricing
| No  | 列名                  | 説明                                                          |
| --- | --------------------- | ------------------------------------------------------------- |
| 1   | project_id            | Google CloudのプロジェクトID                                  |
| 2   | logic_active_price    | アクティブの論理ストレージ課金モデルにかかる料金 (単位: $/月) |
| 3   | logic_long_term_price | 長期保存の論理ストレージ課金モデルにかかる料金 (単位: $/月)   |
| 4   | comp_active_price     | アクティブの物理ストレージ課金モデルにかかる料金 (単位: $/月) |
| 5   | comp_long_term_price  | 長期保存の物理ストレージ課金モデルにかかる料金 (単位: $/月)   |
| 6   | logic_total_price     | 論理ストレージ課金モデルにかかる料金 (単位: $/月)             |
| 7   | comp_total_price      | 物理ストレージ課金モデルにかかる料金 (単位: $/月)             |
| 8   | active_total_price    | アクティブのストレージ全体にかかる料金 (単位: $/月)           |
| 9   | long_term_total_price | 長期保存のストレージ全体にかかる料金 (単位: $/月)             |
| 10  | total_price           | ストレージ全体にかかる料金 (単位: $/月)                       |
| 11  | year                  | 集計日-年                                                     |
| 12  | year_month            | 集計日-年月                                                   |
| 13  | year_month_beginning  | 集計日-月の初日 (例: 2024/01→2023/01/01)                      |
## query_analysis
| No  | 列名        | 説明                              |
| --- | ----------- | --------------------------------- |
| 1   | project_id  | Google CloudのプロジェクトID      |
| 2   | job_id      | BigQueryのジョブID                |
| 3   | account     | ジョブ実行ユーザのメールアドレス  |
| 4   | query       | ジョブの実行クエリ本文            |
| 5   | query_size  | クエリのスキャンサイズ (単位: TB) |
| 6   | cache       | キャッシュヒットの有無            |
| 7   | query_start | クエリ開始時刻                    |
| 8   | query_end   | クエリ終了時刻                    |
| 9   | query_time  | クエリ処理時間 (単位: 秒)         |
| 10  | query_type  | クエリタイプ                      |
| 11  | date        | 集計日                            |
| 12  | year_month  | 集計日-年月                       |
| 13  | year        | 集計日-年                         |
## slot_analysis
| No  | 列名           | 説明                                    |
| --- | -------------- | --------------------------------------- |
| 1   | project_id     | Google CloudのプロジェクトID            |
| 2   | job_id         | BigQueryのジョブID                      |
| 3   | account        | ジョブ実行ユーザのメールアドレス        |
| 4   | reservation_id | BigQuery Editionsの予約ID               |
| 5   | query          | ジョブの実行クエリ本文                  |
| 6   | slot_max       | スロット使用量の予測最大値 (単位: 時間) |
| 7   | slot_min       | スロット使用量の予測最小値 (単位: 時間) |
| 8   | cache          | キャッシュヒットの有無                  |
| 9   | job_start      | BigQueryのジョブ開始時刻                |
| 10  | job_end        | BigQueryのジョブ終了時刻                |
| 11  | job_time       | BigQueryのジョブ処理時間 (単位: 秒)     |
| 12  | query_type     | クエリタイプ                            |
| 13  | date           | 集計日                                  |
| 14  | year_month     | 集計日-年月                             |
| 15  | year           | 集計日-年                               |
## storage_analysis
| No  | 列名                  | 説明                                                                       |
| --- | --------------------- | -------------------------------------------------------------------------- |
| 1   | project_id            | Google CloudのプロジェクトID                                               |
| 2   | dataset_id            | BigQueryのデータセットID                                                   |
| 3   | storage_type          | データセットのストレージタイプ                                             |
| 4   | logic_active          | 論理ストレージ課金モデルのアクティブデータ量 (単位: GB)                    |
| 5   | logic_long_term       | 論理ストレージ課金モデルの長期保存データ量 (単位: GB)                      |
| 6   | comp_active           | 物理ストレージ課金モデルのアクティブデータ量 (単位: GB)                    |
| 7   | comp_long_term        | 物理ストレージ課金モデルの長期保存データ量 (単位: GB)                      |
| 8   | comp_time_travel      | 物理ストレージ課金モデルのタイムトラベル存データ量 (単位: GB)              |
| 9   | comp_fail_safe        | 物理ストレージ課金モデルのフェイルセーフデータ量 (単位: GB)                |
| 10  | diff_logic_active     | 論理ストレージ課金モデルのアクティブ差分データ量 (単位: GB、差分: 1日)     |
| 11  | diff_logic_long_term  | 論理ストレージ課金モデルの長期保存差分データ量 (単位: GB、差分: 1日)       |
| 12  | diff_comp_active      | 物理ストレージ課金モデルのアクティブ差分データ量 (単位: GB、差分: 1日)     |
| 13  | diff_comp_long_term   | 物理ストレージ課金モデルの長期保存差分データ量 (単位: GB、差分: 1日)       |
| 14  | diff_comp_time_travel | 物理ストレージ課金モデルのタイムトラベル差分データ量 (単位: GB、差分: 1日) |
| 15  | diff_comp_fail_safe   | 物理ストレージ課金モデルのフェイルセーフ差分データ量 (単位: GB、差分: 1日) |
| 16  | date                  | 集計日                                                                     |
| 17  | year_month            | 集計日-年月                                                                |
| 18  | year                  | 集計日-年                                                                  |

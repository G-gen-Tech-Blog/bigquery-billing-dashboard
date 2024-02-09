# 利用手順
本利用手順では、株式会社 G-gen が発行する「BigQuery Cost Analyzer」を自社環境にデプロイする手順を解説します。<br>自社の Google Cloud (BigQuery) に設定を行い、Looker Studio のテンプレートを使用しダッシュボードを作成します。
## 1. サービスアカウントの作成
- [サービスアカウントの作成手順](https://cloud.google.com/iam/docs/service-accounts-create?hl=ja#iam-service-accounts-create-console) を参考にサービスアカウントを作成
- 以下、作成するサービスアカウントの設定値

<table>
<thead>
<tr>
<th scope="col">No</th>
<th scope="col">サービスアカウント名</th>
<th scope="col">割り当てるロール</th>
<th scope="col">説明</th>
</tr>
</thead>
<tbody>
<tr>
<td rowspan="4">1</td>
<td rowspan="4">sa-for-bq-job</td>
<td>組織レベルのroles/bigquery.resourceViewe</td>
<td rowspan="4">スケジュールドクエリ用</td>
</tr>
<tr>
<td>組織レベルのroles/bigquery.metadataViewer</td>
</tr>
<tr>
<td>プロジェクトレベルのroles/bigquery.dataEditor</td>
</tr>
<tr>
<td>プロジェクトレベルのroles/bigquery.jobUser</td>
</tr>
<tr>
<td>2</td>
<td>sa-for-looker-studio</td>
<td><a href="https://support.google.com/looker-studio/answer/10835295?hl=ja#zippy=%2C%E3%81%93%E3%81%AE%E8%A8%98%E4%BA%8B%E3%81%AE%E5%86%85%E5%AE%B9">公式ドキュメント</a>を参照</td>
<td>Looker Studio からBigQuery へのアクセス用(任意)</td>
</tr>
</tbody>
</table>

## 2. データセットの作成
- [データセット作成手順](https://cloud.google.com/bigquery/docs/datasets?hl=ja) を参考にデータセットを作成
- 以下、作成するデータセットの設定値

| No  | データセット ID  | ロケーション タイプ | 説明                             |
| --- | ---------------- | ------------------- | -------------------------------- |
| 1   | bigquery_pricing | asia-northeast1     | 料金単価格納用                   |
| 2   | mart             | asia-northeast1     | スケジュールドクエリの結果格納用 |
## 3. 料金単価テーブルの作成
### 3.1 現在の料金単価を確認
以下の公式ドキュメント群から、asia-northeast1の料金単価を確認
- コンピュート料金単価
  - [オンデマンドクエリ料金単価](https://cloud.google.com/bigquery/pricing#:~:text=using%20Reservations.-,On%2Ddemand%20compute%20pricing,-By%20default%2C%20queries)
  - [Standard Edition 料金単価](https://cloud.google.com/bigquery/pricing#:~:text=of%20the%20period.-,Standard%20Edition,-The%20following%20table)
    - Commitment modelが「Pay as you go」の料金を参照
  - [Enterprise Edition 料金単価](https://cloud.google.com/bigquery/pricing#:~:text=1%20minute%20minimum-,Enterprise%20Edition,-The%20following%20table)
    - Commitment modelが「Pay as you go」の料金を参照
  - [Enterprise Plus Edition 料金単価](https://cloud.google.com/bigquery/pricing#:~:text=for%203%20years-,Enterprise%20Plus%20Edition,-The%20following%20table)
    - Commitment modelが「Pay as you go」の料金を参照
- ストレージ料金単価
  - [ストレージ料金単価](https://cloud.google.com/bigquery/pricing#:~:text=Platform%20SKUs%20apply.-,Storage%20pricing,-Storage%20pricing%20is)
### 3.2 テーブルを作成
- [3.1 現在の料金単価を確認](#31-現在の料金単価を確認) で確認した料金単価になるように、以下のSQL文を書き換えてから実行
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
## 4. スケジュールドクエリの作成
- 本サイトから入手したSQLをベースに、スケジュールドクエリを作成 (参考情報: [クエリのスケジューリング](https://cloud.google.com/bigquery/docs/scheduling-queries?hl=ja))
- 以下、作成するスケジュールドクエリの設定値
  - 「スケジュールドクエリ名」、「頻度」は必要に応じて変更をお願いします

| No  | スケジュールドクエリ名         | クエリ内容                         | 頻度             | リージョン      | サービスアカウント |
| --- | ------------------------------ | ---------------------------------- | ---------------- | --------------- | ------------------ |
| 1   | daily_editions_compute_pricing | daily_editions_compute_pricing.sql | 毎日、21:00(UTC) | asia-northeast1 | sa-for-bq-job      |
| 2   | daily_ondemand_compute_pricing | daily_ondemand_compute_pricing.sql | 毎日、21:00(UTC) | asia-northeast1 | sa-for-bq-job      |
| 3   | daily_storage_usage            | daily_storage_usage.sql            | 毎日、21:00(UTC) | asia-northeast1 | sa-for-bq-job      |
| 4   | monthly_storage_pricing        | monthly_storage_pricing.sql        | 毎日、21:30(UTC) | asia-northeast1 | sa-for-bq-job      |
| 5   | query_analysis                 | query_analysis.sql                 | 毎日、21:00(UTC) | asia-northeast1 | sa-for-bq-job      |
| 6   | slot_analysis                  | slot_analysis.sql                  | 毎日、21:00(UTC) | asia-northeast1 | sa-for-bq-job      |
| 7   | storage_analysis               | storage_analysis.sql               | 毎日、21:00(UTC) | asia-northeast1 | sa-for-bq-job      |
## 5. ダッシュボードの作成
### 5.1 Looker Studioのテンプレートを開く
- [テンプレートURL](https://lookerstudio.google.com/u/0/reporting/ac95599a-da77-42f0-8c17-f65ca9ee94d5/preview) 
### 5.2 ダッシュボードの参照テーブルを変更
1.  右上の 「自分のデータを使用」をクリック
2.  [4. スケジュールドクエリの作成](#4-スケジュールドクエリの作成) で作成したマートテーブルを選択し、「追加」をクリック
### 5.3 サービスアカウントの設定（任意）
- BigQueryのテーブルへのアクセスをサービスアカウント経由で行いたい場合は、[1. サービスアカウントの作成](#1-サービスアカウントの作成) に記載の「sa-for-looker-studio」を設定
- 設定方法は、[公式ドキュメント](https://support.google.com/looker-studio/answer/10835295?hl=ja&ref_topic=10388739)を参照
<br>
<br>

# 運用・メンテナンス
BigQuery の費用が改定された場合「3. 料金単価テーブルの作成」で作成した料金単価テーブルを更新し、ダッシュボードで適切な費用が表示されるよう更新を行います。
## 料金単価テーブルの更新
- 「3.1 現在の料金単価を確認」を参照し、最新の料金単価を確認する
- 更新されている単価がある場合、コンピュート、ストレージ料金単価テーブルそれぞれに新たな単価のレコードを追加
  - レコード追加方法は、[3.2 テーブルを作成](#32-テーブルを作成)を参照
<br>
<br>

# BigQueryテーブル説明
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

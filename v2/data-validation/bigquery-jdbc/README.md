# BigQuery JDBC

Builds a fat jar from [BQ JDBC zip](https://cloud.google.com/bigquery/docs/reference/odbc-jdbc-drivers#current_jdbc_driver).

Google issue tracker: https://issuetracker.google.com/issues/180413368

```shell
wget https://storage.googleapis.com/simba-bq-release/jdbc/SimbaBigQueryJDBC42-1.3.2.1003.zip
unzip SimbaBigQueryJDBC42-1.3.2.1003.zip -d bigquery-jdbc 
rm -rf SimbaBigQueryJDBC42-1.3.2.1003.zip
./gradlew build
```

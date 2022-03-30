/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package spendreport;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.functions.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Skeleton code for the datastream walkthrough
 */
public class FlinkTestJob {
	private static final Logger LOG = LoggerFactory.getLogger(FlinkTestJob.class);

	private static class DataSource extends RichParallelSourceFunction<Tuple3<Long, String, Integer>> {
		private volatile boolean isRunning = true;

		@Override
		public void run(SourceContext<Tuple3<Long, String, Integer>> ctx) throws Exception {
			Random random = new Random();
			while (isRunning) {
				Thread.sleep((getRuntimeContext().getIndexOfThisSubtask() + 1) * 1000 * 5);
				String key = "类别" + (char) ('A' + random.nextInt(3));
				int value = random.nextInt(10) + 1;

				System.out.println(String.format("Emits\t(%d, %s, %d)", value%10, key, value));
				ctx.collect(new Tuple3<Long, String, Integer>((long)value%10,key,value));
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}
	}

	public static void main(String[] args) throws Exception {
		/*
		EnvironmentSettings settings = EnvironmentSettings
				.newInstance()
				.inStreamingMode()
				//.inBatchMode()
				.build();
		TableEnvironment tableEnv = TableEnvironment.create(settings);
		tableEnv.executeSql("CREATE TEMPORARY TABLE SourceTable(f0 STRING) WITH ( 'connector' = 'datagen','rows-per-second'='1','fields.f0.length'='10' )");
		tableEnv.executeSql("CREATE TEMPORARY TABLE SinkTable(f0 STRING) WITH ( 'connector' = 'blackhole' ) ");
		Table table2 = tableEnv.from("SourceTable");
		Table table3 = tableEnv.sqlQuery("SELECT * FROM SourceTable ");
//		table3.execute().print();
		TableResult tableResult = table2.executeInsert("SinkTable");
		tableResult.print();
		*/

		/*
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setMaxParallelism(256);
		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
		 */

		/*
		DataStream<Row> dataStream = env.fromElements(
				Row.of("Alice", 12),
				Row.of("Bob", 10),
				Row.of("Alice", 100));
		Table inputTable = tableEnv.fromDataStream(dataStream).as("name", "score");
		tableEnv.createTemporaryView("InputTable", inputTable);

		// interpret the insert-only Table as a DataStream again
		Table resultTable = tableEnv.sqlQuery("SELECT UPPER(name) FROM InputTable");
		DataStream<Row> resultStream = tableEnv.toDataStream(resultTable);
		// interpret the updating Table as a changelog DataStream
//		Table resultTable = tableEnv.sqlQuery("SELECT name, SUM(score) FROM InputTable GROUP BY name");
//		DataStream<Row> resultStream = tableEnv.toChangelogStream(resultTable);

		resultStream.print();
		env.execute();
		 */

		/*
		DataStream<Row> dataStream =
				env.fromElements(
						Row.ofKind(RowKind.INSERT, "Alice", 12),
						Row.ofKind(RowKind.INSERT, "Bob", 5),
						Row.ofKind(RowKind.UPDATE_BEFORE, "Alice", 12),
						Row.ofKind(RowKind.UPDATE_AFTER, "Alice", 100));
		Table table = tableEnv.fromChangelogStream(dataStream);
		tableEnv.createTemporaryView("InputTable", table);
		tableEnv.from("InputTable").printSchema();
		tableEnv
				.executeSql("SELECT f0 AS name, SUM(f1) AS score FROM InputTable GROUP BY f0")
				.print();
		 */

		/*
		DataStream<Row> dataStream = env.fromElements(
				Row.of("john", 35),
				Row.of("sarah", 32));
		Table table = tableEnv.fromDataStream(dataStream).as("name", "age");
		DataStream<Row> dsRow = tableEnv.toAppendStream(table, Row.class);
		dsRow.print();

		TupleTypeInfo<Tuple2<String, Integer>> tupleType = new TupleTypeInfo<>(Types.STRING(), Types.INT());
		DataStream<Tuple2<String, Integer>> dsTuple = tableEnv.toAppendStream(table, tupleType);
		dsTuple.print();

		// True is INSERT, false is DELETE.
		DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv.toRetractStream(table, Row.class);
		retractStream.print();

		env.execute();
		 */

		/*
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

		DataStream<Tuple3<Long, String, Integer>> ds = env.addSource(new DataSource());
		 */


		/*
		Table table = tableEnv.fromDataStream(ds).as("user","product","amount");
		Table result = tableEnv.sqlQuery(
				"SELECT SUM(amount) FROM " + table + " WHERE product LIKE '%A%'");
		result.execute().print();
//		DataStream<Row> resultStream = tableEnv.toChangelogStream(result);
//		resultStream.print();
//		env.execute();
		 */

		/*
		tableEnv.createTemporaryView("Orders", ds);
		Table result2 = tableEnv.sqlQuery(
				"SELECT f1, f2 FROM Orders WHERE f1 LIKE '%A%'");
		result2.execute().print();
		 */

		/*
		tableEnv.createTemporaryView("Orders", ds);
		tableEnv.connect(new FileSystem().path("/Users/linechina/eclipse-workspace/flink/frauddetection/test.csv"))
				.withFormat(new Csv().fieldDelimiter('|').deriveSchema())
				.withSchema(new Schema().field("product",DataTypes.STRING())
						.field("amount", DataTypes.INT()))
				.createTemporaryTable("RubberOrders");
		TableResult tableResult1 = tableEnv.executeSql(
				"INSERT INTO RubberOrders SELECT f1, f2 FROM Orders WHERE f1 LIKE '%A%'");
		System.out.println(tableResult1.getJobClient().get().getJobStatus());
		 */


		System.out.println(System.getenv("HADOOP_CLASSPATH"));
//		System.setProperty("log.file","/Users/linechina/eclipse-workspace/flink/flink-test/logs/msg.log");

//		StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("10.253.0.63",8090);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		/*
		Configuration configuration = new Configuration();
//		configuration.setInteger("rest.port", 8082);
		configuration.setString(RestOptions.BIND_PORT,"8081-8089");
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
		 */
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		EnvironmentSettings tableEnvSettings = EnvironmentSettings.newInstance()
				.useBlinkPlanner()
				.inStreamingMode()
//				.inBatchMode()
				.build();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,tableEnvSettings);
//		TableEnvironment tableEnv = TableEnvironment.create(tableEnvSettings);
//		tableEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);
		tableEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(10));
		tableEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.MIN_PAUSE_BETWEEN_CHECKPOINTS,Duration.ofSeconds(60));
		tableEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_TIMEOUT,Duration.ofSeconds(60));
		tableEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.EXTERNALIZED_CHECKPOINT, CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
		tableEnv.getConfig().getConfiguration().setString("table.dynamic-table-options.enabled","true");

		String catalogName = "mhive";
//		String hiveConfPath = "/Users/linechina/eclipse-workspace/flink/flink-test/conf/";
		String hiveConfPath = "/home/ec2-user/spark-3.0.0-bin-3.1.1/conf/";
		HiveCatalog catalog = new HiveCatalog(catalogName,"myhive",hiveConfPath,"3.1.1");
		tableEnv.registerCatalog(catalogName,catalog);
		tableEnv.useCatalog(catalogName);

		System.out.println(tableEnv.listDatabases());

		tableEnv.executeSql("show tables").print();

		tableEnv.useDatabase("myhive");

		tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
		tableEnv.executeSql("DROP table if exists cdn_access_statistic");
		tableEnv.executeSql("CREATE TABLE cdn_access_statistic ("+
				"province string,"+
				"access_count bigint,"+
				"total_download bigint,"+
				"download_speed DOUBLE"+
				") PARTITIONED BY (dt STRING, hr STRING, ts_minute STRING) STORED AS parquet TBLPROPERTIES ("+
				"'streaming-source.enable' = 'true',"+
				"'partition.time-extractor.timestamp-pattern'='$dt $hr:$ts_minute:00',"+
				"'sink.partition-commit.trigger'='partition-time',"+
				"'sink.partition-commit.delay'='1 min',"+
				"'sink.partition-commit.policy.kind'='metastore,success-file'"+
				") ");


		/*
		StatementSet statementSet = tableEnv.createStatementSet();
		statementSet.addInsertSql("insert into cdn_access_statistic values('ss',10,100,3.1,'2022-03-29','09','01')");
		TableResult statementResult = statementSet.execute();
		System.out.println(statementResult.getJobClient().get().getJobStatus().get());

		TableResult result = tableEnv.sqlQuery("select * from myhive.cdn_access_statistic").execute();
		LOG.warn("start query hive");
		result.print();
		LOG.warn("end query hive");
//		try (CloseableIterator<Row> it = result.collect()) {
//			while(it.hasNext()) {
//				Row row = it.next();
//				System.out.println(row.toString());
//			}
//		}
		*/

		tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
		tableEnv.executeSql("DROP table if exists cdn_access_log");
		tableEnv.executeSql("CREATE TABLE cdn_access_log ("+
				"uuid VARCHAR,"+
				"client_ip VARCHAR,"+
				"request_time BIGINT,"+
				"response_size BIGINT,"+
				"uri VARCHAR,"+
				"event_ts BIGINT,"+
				"ts_ltz as TO_TIMESTAMP_LTZ(event_ts/1000000, 3),"+
				"WATERMARK FOR ts_ltz AS ts_ltz - INTERVAL '15' SECOND"+
				") WITH ("+
				"'connector' = 'kafka',"+
				"'topic' = 'cdn_events',"+
				"'properties.bootstrap.servers' = '10.253.0.102:9092',"+
				"'properties.group.id' = 'testGroup',"+
				"'format' = 'json',"+
				"'scan.startup.mode' = 'latest-offset'"+
				")");
//		TableResult kafkaResult = tableEnv.executeSql("select count(*) from cdn_access_log");
//		kafkaResult.print();

		tableEnv.registerFunction("ip_to_country", new IpToCountryFunction());
		StatementSet stmtSet = tableEnv.createStatementSet();
		stmtSet.addInsertSql(
				"INSERT INTO cdn_access_statistic"+
				"(select ip_to_country(client_ip) as country,"+
				"count(uuid) as access_count,"+
				"sum(response_size) as total_download,"+
				"sum(response_size) * 1.0 / sum(request_time) as download_speed,"+
				"DATE_FORMAT(window_start, 'yyyy-MM-dd'), DATE_FORMAT(window_start, 'HH'),DATE_FORMAT(window_start, 'mm')"+
				"from table("+
				"tumble(table cdn_access_log,descriptor(ts_ltz),interval '1' minutes))"+
				"group by ip_to_country(client_ip),window_start,window_end)");
		TableResult kafkaResult2 = stmtSet.execute();
		System.out.println(kafkaResult2.getJobClient().get().getJobStatus());

	}

	public static class IpToCountryFunction extends ScalarFunction{
		public String eval(String ip) {
			List<String> countries = Arrays.asList("USA","China");
			int last_dot = ip.lastIndexOf(".");
			int country_index = Integer.parseInt(ip.substring(last_dot+1)) % countries.size();
			return countries.get(country_index);
		}

	}
}




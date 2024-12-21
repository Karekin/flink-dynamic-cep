package org.apache.flink.cep.examples.cep;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEPUtils;
import org.apache.flink.cep.TimeBehaviour;
import org.apache.flink.cep.discover.JdbcPeriodicRuleDiscovererFactory;
import org.apache.flink.connector.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;
import java.util.Collections;

/**
 * Flink CEP 引擎支持动态多规则示例
 *
 * @author shirukai
 */
public class DynamicCepExamples {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Row> events = env.fromElements(
                Row.of("device-1", 1, 50.0, 5600L, 1705307073000L), // 2024-01-15 12:00:00
                Row.of("device-1", 1, 56.4, 6430L, 1705307080000L), // 2024-01-15 12:00:40
                Row.of("device-1", 1, 60.8, 6670L, 1705307085000L), // 2024-01-15 12:01:25
                Row.of("device-1", 0, 60.8, 6670L, 1705307205000L)  // 2024-01-15 12:06:45
        ).returns(Types.ROW_NAMED(new String[]{"id", "action", "temp", "rpm", "detection_time"}, Types.STRING, Types.INT, Types.DOUBLE, Types.LONG, Types.LONG));

//
//        // 分配时间戳和水印
//        events = events.assignTimestampsAndWatermarks(
//                WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofSeconds(10))
//                        .withTimestampAssigner((event, timestamp) -> event.getFieldAs("detection_time"))
//        );


        SingleOutputStreamOperator<Row> alarms = CEPUtils.dynamicCepRules(
                events.keyBy((KeySelector<Row, String>) value -> value.getFieldAs("id")),
                new JdbcPeriodicRuleDiscovererFactory(
                        JdbcConnectorOptions.builder()
                                .setTableName("public.cep_rules")
                                .setDriverName("org.postgresql.Driver")
                                .setDBUrl("jdbc:postgresql://127.0.0.1:5432/riskcontrol")
                                .setUsername("root")
                                .setPassword("root")
                                .build(),
                        3,
                        "cep",
                        Collections.emptyList(),
                        Duration.ofMinutes(1).toMillis()),
                TimeBehaviour.ProcessingTime,
                Types.ROW_NAMED(new String[]{"id", "rpm_avg", "temp_avg", "detection_time", "rpm_threshold", "temp_threshold"},
                        Types.STRING, Types.DOUBLE, Types.DOUBLE, Types.LONG, Types.DOUBLE, Types.DOUBLE),
                "cep-test",
                "/",
                false
        );

        alarms.print();

        env.execute("DynamicCepExamples");

    }

}

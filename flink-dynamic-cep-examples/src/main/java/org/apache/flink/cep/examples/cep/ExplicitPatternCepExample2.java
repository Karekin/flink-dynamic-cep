package org.apache.flink.cep.examples.cep;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Flink CEP 显式定义 Pattern 示例
 *
 * @author shirukai
 */
public class ExplicitPatternCepExample2 {
    public static void main(String[] args) throws Exception {
        // 设置 Flink 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 事件流数据
        DataStream<Row> events = env.fromElements(
                        Row.of("device-1", 50.0, 5600L, 1705307073000L),
                        Row.of("device-1", 60.8, 6670L, 1705307085000L),
                        Row.of("device-1", 56.4, 6430L, 1705307040000L),
                        Row.of("device-1", 60.8, 6670L, 1705307205000L)
                )
                .returns(Types.ROW_NAMED(new String[]{"id", "temp", "rpm", "detection_time"},
                        Types.STRING, Types.DOUBLE, Types.LONG, Types.LONG));

        // 分配时间戳和水印
        events = events.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner((event, timestamp) -> event.getFieldAs("detection_time"))
        );

        // 显式定义 Pattern
        Pattern<Row, ?> pattern = Pattern.<Row>begin("start")
                .where(SimpleCondition.of(event -> (double) event.getFieldAs("temp") > 55.0 &&
                        (long) event.getFieldAs("rpm") > 6000L))
                .within(Time.seconds(120)); // 120秒的时间窗口

        // 将 Pattern 应用到事件流
//        SingleOutputStreamOperator<Row> alarms = CEP.pattern(
//                        events.keyBy((KeySelector<Row, String>) value -> value.getFieldAs("id")),
//                        pattern)
//                .select(new PatternSelectFunction<Row, Row>() {
//                    @Override
//                    public Row select(Map<String, List<Row>> pattern) throws Exception {
//                        List<Row> startEvents = pattern.get("start");
//                        // 计算平均值
//                        double avgRpm = startEvents.stream().mapToLong(row -> row.getFieldAs("rpm")).average().orElse(0.0);
//                        double avgTemp = startEvents.stream().mapToDouble(row -> row.getFieldAs("temp")).average().orElse(0.0);
//                        return Row.of(startEvents.get(0).getFieldAs("id"), avgRpm, avgTemp, startEvents.get(0).getFieldAs("detection_time"));
//                    }
//                });

        SingleOutputStreamOperator<Row> alarms = CEP.pattern(
                        events.keyBy((KeySelector<Row, String>) value -> value.getFieldAs("id")),
                        pattern)
                .process(new MyPatternProcessFunction());

        // 打印匹配的结果
        alarms.print("Matched Events");

        // 执行作业
        env.execute("ExplicitPatternCepExample");
    }
}

package com.hauesoftware.traffic.analysis;

import com.hauesoftware.traffic.model.TrafficRecord;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * 车流量分析 — 30秒滚动窗口统计各卡口和各区域通过的车辆数
 */
public class TrafficFlowAnalysis {

    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern("HH:mm:ss").withZone(ZoneId.systemDefault());

    public static void analyze(DataStream<TrafficRecord> stream) {

        // 按卡口统计
        stream.keyBy(TrafficRecord::getCheckpointId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .process(new FlowWindowFunction("车流量-卡口"))
                .print();

        // 按区域统计
        stream.keyBy(TrafficRecord::getDistrict)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .process(new FlowWindowFunction("车流量-区域"))
                .print();
    }

    private static class FlowWindowFunction
            extends ProcessWindowFunction<TrafficRecord, String, String, TimeWindow> {

        private final String label;

        FlowWindowFunction(String label) {
            this.label = label;
        }

        @Override
        public void process(String key, Context context, Iterable<TrafficRecord> records,
                            Collector<String> out) {
            long count = 0;
            for (TrafficRecord ignored : records) {
                count++;
            }

            String start = FORMATTER.format(Instant.ofEpochMilli(context.window().getStart()));
            String end = FORMATTER.format(Instant.ofEpochMilli(context.window().getEnd()));

            out.collect(String.format("[%s] 窗口: %s ~ %s, %s: %s, 通过车辆: %d",
                    label, start, end,
                    label.contains("卡口") ? "卡口" : "区域",
                    key, count));
        }
    }
}

package com.hauesoftware.traffic.analysis;

import com.hauesoftware.traffic.model.TrafficRecord;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 车辆分布分析 — 30秒滚动窗口统计车辆类型和区域分布（含占比）
 */
public class VehicleDistributionAnalysis {

    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern("HH:mm:ss").withZone(ZoneId.systemDefault());

    public static void analyze(DataStream<TrafficRecord> stream) {

        stream.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .process(new DistributionProcessFunction())
                .print();
    }

    private static class DistributionProcessFunction
            extends ProcessAllWindowFunction<TrafficRecord, String, TimeWindow> {

        @Override
        public void process(Context context, Iterable<TrafficRecord> records,
                            Collector<String> out) {
            Map<String, Integer> typeCounts = new LinkedHashMap<>();
            Map<String, Integer> districtCounts = new LinkedHashMap<>();
            int total = 0;

            for (TrafficRecord r : records) {
                typeCounts.merge(r.getVehicleType(), 1, Integer::sum);
                districtCounts.merge(r.getDistrict(), 1, Integer::sum);
                total++;
            }

            if (total == 0) return;

            long start = context.window().getStart();
            long end = context.window().getEnd();
            String windowStr = String.format("%s ~ %s",
                    FORMATTER.format(Instant.ofEpochMilli(start)),
                    FORMATTER.format(Instant.ofEpochMilli(end)));

            // 车辆类型分布
            StringBuilder sb = new StringBuilder();
            sb.append("[车辆类型分布] 窗口: ").append(windowStr).append(" | ");
            appendDistribution(sb, typeCounts, total);
            out.collect(sb.toString());

            // 区域分布
            sb = new StringBuilder();
            sb.append("[区域分布]     窗口: ").append(windowStr).append(" | ");
            appendDistribution(sb, districtCounts, total);
            out.collect(sb.toString());
        }

        private void appendDistribution(StringBuilder sb, Map<String, Integer> counts, int total) {
            boolean first = true;
            for (var entry : counts.entrySet()) {
                if (!first) sb.append(", ");
                sb.append(entry.getKey())
                        .append(": ")
                        .append(entry.getValue())
                        .append(" (")
                        .append(String.format("%.0f%%", 100.0 * entry.getValue() / total))
                        .append(")");
                first = false;
            }
        }
    }
}

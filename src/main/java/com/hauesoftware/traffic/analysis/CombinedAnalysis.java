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
import java.util.LinkedHashMap;
import java.util.Map;

public class CombinedAnalysis {

    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern("HH:mm:ss").withZone(ZoneId.systemDefault());

    public static void analyze(DataStream<TrafficRecord> stream) {

        stream.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(15)))
                .process(new ReportFunction())
                .print();
    }

    private static class ReportFunction
            extends ProcessAllWindowFunction<TrafficRecord, String, TimeWindow> {

        @Override
        public void process(Context context, Iterable<TrafficRecord> records,
                            Collector<String> out) {
            Map<String, Integer> checkpointCounts = new LinkedHashMap<>();
            Map<String, String> checkpointNames = new LinkedHashMap<>();
            Map<String, Integer> districtCounts = new LinkedHashMap<>();
            Map<String, Integer> typeCounts = new LinkedHashMap<>();
            int total = 0;

            for (TrafficRecord r : records) {
                checkpointCounts.merge(r.getCheckpointId(), 1, Integer::sum);
                checkpointNames.putIfAbsent(r.getCheckpointId(), r.getCheckpointName());
                districtCounts.merge(r.getDistrict(), 1, Integer::sum);
                typeCounts.merge(r.getVehicleType(), 1, Integer::sum);
                total++;
            }

            if (total == 0) return;

            String start = FORMATTER.format(Instant.ofEpochMilli(context.window().getStart()));
            String end = FORMATTER.format(Instant.ofEpochMilli(context.window().getEnd()));

            StringBuilder sb = new StringBuilder();
            sb.append("\n");
            sb.append("==================== 城市交通实时监控平台 ====================\n");
            sb.append("窗口: ").append(start).append(" ~ ").append(end).append("\n");
            sb.append("------------------------------------------------------------------\n");

            // 一、车流量统计（按卡口）
            sb.append("一、车流量统计（按卡口）\n");
            for (var entry : checkpointCounts.entrySet()) {
                String name = checkpointNames.getOrDefault(entry.getKey(), "");
                sb.append(String.format("  %s %-26s: %4d 辆\n",
                        entry.getKey(), name, entry.getValue()));
            }

            // 二、车流量统计（按区域）
            sb.append("二、车流量统计（按区域）\n");
            for (var entry : districtCounts.entrySet()) {
                sb.append(String.format("  %-6s : %4d 辆\n", entry.getKey(), entry.getValue()));
            }

            // 三、车辆类型分布
            sb.append("三、车辆类型分布（共 ").append(total).append(" 辆）\n");
            for (var entry : typeCounts.entrySet()) {
                sb.append(String.format("  %-10s : %4d (%2.0f%%)\n",
                        entry.getKey(), entry.getValue(),
                        100.0 * entry.getValue() / total));
            }

            // 四、区域分布
            sb.append("四、区域分布（共 ").append(total).append(" 辆）\n");
            for (var entry : districtCounts.entrySet()) {
                sb.append(String.format("  %-6s : %4d (%2.0f%%)\n",
                        entry.getKey(), entry.getValue(),
                        100.0 * entry.getValue() / total));
            }

            sb.append("==================================================================\n");

            out.collect(sb.toString());
        }
    }
}

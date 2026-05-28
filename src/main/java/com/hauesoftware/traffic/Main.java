package com.hauesoftware.traffic;

import com.hauesoftware.traffic.analysis.CombinedAnalysis;
import com.hauesoftware.traffic.model.TrafficRecord;
import com.hauesoftware.traffic.source.TrafficDataGenerator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 城市交通实时监控平台 — 入口
 * 启动后实时生成虚拟城市交通数据，进行车流量分析和车辆分布分析，结果输出到控制台。
 */
public class Main {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<TrafficRecord> trafficStream = env.addSource(new TrafficDataGenerator())
                .name("Traffic-Data-Generator");

        // 原始数据抽样输出到 stderr（每 3 条输出 1 条，简洁格式）
        trafficStream.filter(r -> Math.abs(r.hashCode()) % 3 == 0)
                .map(r -> String.format("[数据] %s %s %s %.1fkm/h",
                        r.getCheckpointId(), r.getVehicleType(),
                        r.getVehicleId(), r.getSpeed()))
                .printToErr();

        // 统一分析（15秒窗口，格式化报表输出到 stdout）
        CombinedAnalysis.analyze(trafficStream);

        System.out.println("========================================");
        System.out.println("  城市交通实时监控平台启动中...");
        System.out.println("  虚拟数据源: 郑州市 10个卡口");
        System.out.println("  分析窗口: 15秒滚动窗口");
        System.out.println("========================================");

        env.execute("TrafficStatusAnalysing");
    }
}

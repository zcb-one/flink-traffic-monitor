package com.hauesoftware.traffic;

import com.hauesoftware.traffic.analysis.TrafficFlowAnalysis;
import com.hauesoftware.traffic.analysis.VehicleDistributionAnalysis;
import com.hauesoftware.traffic.model.TrafficRecord;
import com.hauesoftware.traffic.source.TrafficDataGenerator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 城市交通实时监控平台 — 入口
 *
 * 启动后实时生成虚拟城市交通数据，进行车流量分析和车辆分布分析，结果输出到控制台。
 */
public class Main {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<TrafficRecord> trafficStream = env.addSource(new TrafficDataGenerator())
                .name("Traffic-Data-Generator");

        // 原始数据抽样输出（每 5 条输出 1 条，避免刷屏）
        trafficStream.filter(r -> r.hashCode() % 5 == 0)
                .map(r -> "[原始数据] " + r.toString())
                .print();

        // 车流量分析
        TrafficFlowAnalysis.analyze(trafficStream);

        // 车辆分布分析
        VehicleDistributionAnalysis.analyze(trafficStream);

        System.out.println("========================================");
        System.out.println("  城市交通实时监控平台启动中...");
        System.out.println("  虚拟数据源: 郑州市 5个卡口, 每秒1~5条");
        System.out.println("  分析窗口: 30秒滚动窗口");
        System.out.println("========================================");

        env.execute("TrafficStatusAnalysing");
    }
}

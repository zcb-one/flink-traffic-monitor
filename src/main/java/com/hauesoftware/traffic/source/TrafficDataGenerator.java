package com.hauesoftware.traffic.source;

import com.hauesoftware.traffic.config.CityConfig;
import com.hauesoftware.traffic.config.CityConfig.Checkpoint;
import com.hauesoftware.traffic.model.TrafficRecord;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * 数据构造器 — 模拟城市交通卡口的实时通行数据
 * 每秒随机生成 1~5 条通行记录
 */
public class TrafficDataGenerator implements SourceFunction<TrafficRecord> {

    private volatile boolean running = true;
    private final Random random = new Random();

    private static final String[] VEHICLE_TYPES = {"小轿车", "面包车", "公交车", "摩托车"};
    // 权重: 小轿车 60%, 面包车 15%, 公交车 10%, 摩托车 15%
    private static final int[] TYPE_WEIGHTS = {60, 15, 10, 15};
    private static final int TYPE_WEIGHT_SUM;
    static {
        int sum = 0;
        for (int w : TYPE_WEIGHTS) sum += w;
        TYPE_WEIGHT_SUM = sum;
    }

    private static final char[] LETTERS = "ABCDEFGHJKLMNPQRSTUVWXYZ".toCharArray();

    @Override
    public void run(SourceContext<TrafficRecord> ctx) throws Exception {
        while (running) {
            TrafficRecord record = generateRecord();
            ctx.collect(record);

            // 随机间隔 200~800ms，平均每秒约 2~5 条
            int delay = 200 + random.nextInt(600);
            Thread.sleep(delay);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    private TrafficRecord generateRecord() {
        Checkpoint cp = CityConfig.CHECKPOINTS.get(random.nextInt(CityConfig.CHECKPOINTS.size()));
        String vehicleType = randomType();

        // 车型限速上限
        double typeMax;
        switch (vehicleType) {
            case "van": typeMax = 60; break;
            case "bus": typeMax = 50; break;
            default:    typeMax = 120;
        }

        double speed = randomSpeed(cp.speedLimit, typeMax);

        return new TrafficRecord(
                randomPlate(),
                vehicleType,
                cp.id,
                cp.name,
                cp.longitude,
                cp.latitude,
                cp.district,
                System.currentTimeMillis(),
                speed,
                1 + random.nextInt(4)
        );
    }

    /**
     * 电子眼测速模型：85%正常、10%轻微超速、5%严重超速，受车型上限约束
     */
    private double randomSpeed(int limit, double typeMax) {
        double effectiveLimit = Math.min(limit, typeMax);
        double factor;
        double r = random.nextDouble() * 100;

        if (r < 85) {
            // 正常行驶: 30%~100% 限速
            factor = 0.3 + random.nextDouble() * 0.7;
        } else if (r < 95) {
            // 轻微超速: 100%~120% 限速
            factor = 1.0 + random.nextDouble() * 0.2;
        } else {
            // 严重超速: 120%~150% 限速
            factor = 1.2 + random.nextDouble() * 0.3;
        }

        return Math.round(effectiveLimit * factor * 10.0) / 10.0;
    }

    private String randomPlate() {
        char letter = LETTERS[random.nextInt(LETTERS.length)];
        int digits = 10000 + random.nextInt(90000);
        return "豫A·" + letter + digits;
    }

    private String randomType() {
        int r = random.nextInt(TYPE_WEIGHT_SUM);
        int cumulative = 0;
        for (int i = 0; i < VEHICLE_TYPES.length; i++) {
            cumulative += TYPE_WEIGHTS[i];
            if (r < cumulative) return VEHICLE_TYPES[i];
        }
        return VEHICLE_TYPES[0];
    }
}

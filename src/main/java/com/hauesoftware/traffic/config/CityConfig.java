package com.hauesoftware.traffic.config;

import com.hauesoftware.traffic.model.TrafficRecord;
import java.util.*;

/**
 * 郑州市10个卡口的静态配置（5区，每区2个）
 */
public class CityConfig {

    public static class Checkpoint {
        public final String id;
        public final String name;
        public final double longitude;
        public final double latitude;
        public final String district;
        public final int speedLimit;

        Checkpoint(String id, String name, double lng, double lat, String district, int speedLimit) {
            this.id = id;
            this.name = name;
            this.longitude = lng;
            this.latitude = lat;
            this.district = district;
            this.speedLimit = speedLimit;
        }
    }

    public static final List<Checkpoint> CHECKPOINTS = List.of(
        // 金水区
        new Checkpoint("CP_001", "金水路与未来路交叉口", 113.682, 34.763, "金水区", 60),
        new Checkpoint("CP_002", "花园路与农业路交叉口", 113.670, 34.788, "金水区", 60),
        // 中原区
        new Checkpoint("CP_003", "中原路与嵩山路交叉口", 113.628, 34.748, "中原区", 60),
        new Checkpoint("CP_004", "建设路与桐柏路交叉口", 113.618, 34.755, "中原区", 50),
        // 二七区
        new Checkpoint("CP_005", "大学路与航海路交叉口", 113.638, 34.718, "二七区", 50),
        new Checkpoint("CP_006", "嵩山路与南三环交叉口", 113.630, 34.710, "二七区", 70),
        // 管城区
        new Checkpoint("CP_007", "紫荆山路与东大街交叉口", 113.675, 34.750, "管城区", 40),
        new Checkpoint("CP_008", "商城路与城东路交叉口", 113.682, 34.752, "管城区", 40),
        // 惠济区
        new Checkpoint("CP_009", "文化路与开元路交叉口", 113.625, 34.828, "惠济区", 60),
        new Checkpoint("CP_010", "天河路与开元路交叉口", 113.608, 34.828, "惠济区", 60)
    );

    /** 索引，方便按ID快速查找卡口 */
    public static final Map<String, Checkpoint> CHECKPOINT_MAP = new HashMap<>();
    static {
        for (Checkpoint cp : CHECKPOINTS) {
            CHECKPOINT_MAP.put(cp.id, cp);
        }
    }

    /** 所有区域列表（去重） */
    public static final List<String> DISTRICTS = CHECKPOINTS.stream()
            .map(cp -> cp.district)
            .distinct()
            .toList();
}

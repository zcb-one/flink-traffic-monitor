package com.hauesoftware.traffic.model;

public class TrafficRecord {
    private String vehicleId;
    private String vehicleType;
    private String checkpointId;
    private String checkpointName;
    private double longitude;
    private double latitude;
    private String district;
    private long passTime;
    private double speed;
    private int laneNumber;

    public TrafficRecord() {}

    public TrafficRecord(String vehicleId, String vehicleType, String checkpointId,
                         String checkpointName, double longitude, double latitude,
                         String district, long passTime, double speed, int laneNumber) {
        this.vehicleId = vehicleId;
        this.vehicleType = vehicleType;
        this.checkpointId = checkpointId;
        this.checkpointName = checkpointName;
        this.longitude = longitude;
        this.latitude = latitude;
        this.district = district;
        this.passTime = passTime;
        this.speed = speed;
        this.laneNumber = laneNumber;
    }

    public String getVehicleId() { return vehicleId; }
    public void setVehicleId(String vehicleId) { this.vehicleId = vehicleId; }

    public String getVehicleType() { return vehicleType; }
    public void setVehicleType(String vehicleType) { this.vehicleType = vehicleType; }

    public String getCheckpointId() { return checkpointId; }
    public void setCheckpointId(String checkpointId) { this.checkpointId = checkpointId; }

    public String getCheckpointName() { return checkpointName; }
    public void setCheckpointName(String checkpointName) { this.checkpointName = checkpointName; }

    public double getLongitude() { return longitude; }
    public void setLongitude(double longitude) { this.longitude = longitude; }

    public double getLatitude() { return latitude; }
    public void setLatitude(double latitude) { this.latitude = latitude; }

    public String getDistrict() { return district; }
    public void setDistrict(String district) { this.district = district; }

    public long getPassTime() { return passTime; }
    public void setPassTime(long passTime) { this.passTime = passTime; }

    public double getSpeed() { return speed; }
    public void setSpeed(double speed) { this.speed = speed; }

    public int getLaneNumber() { return laneNumber; }
    public void setLaneNumber(int laneNumber) { this.laneNumber = laneNumber; }

    @Override
    public String toString() {
        return String.format("TrafficRecord{车牌=%s, 类型=%s, 卡口=%s(%s), 位置=(%.4f,%.4f), 区域=%s, 时间=%d, 速度=%.1f, 车道=%d}",
                vehicleId, vehicleType, checkpointId, checkpointName,
                longitude, latitude, district, passTime, speed, laneNumber);
    }
}

package org.data.meta.hive.model;

public class PartitionInfo {
    private String partitionName;
    private long createTime;
    private long lastAccessTime;
    private String dataLocation;
    private int bucketNum;
    private String partitionPath;

    public PartitionInfo() {
    }

    public String getPartitionName() {
        return this.partitionName;
    }

    public void setPartitionName(String partitionName) {
        this.partitionName = partitionName;
    }

    public long getCreateTime() {
        return this.createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public long getLastAccessTime() {
        return this.lastAccessTime;
    }

    public void setLastAccessTime(long lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
    }

    public String getDataLocation() {
        return this.dataLocation;
    }

    public void setDataLocation(String dataLocation) {
        this.dataLocation = dataLocation;
    }

    public int getBucketNum() {
        return this.bucketNum;
    }

    public void setBucketNum(int bucketNum) {
        this.bucketNum = bucketNum;
    }

    public String getPartitionPath() {
        return this.partitionPath;
    }

    public void setPartitionPath(String partitionPath) {
        this.partitionPath = partitionPath;
    }
}

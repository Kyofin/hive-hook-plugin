package org.data.meta.hive.model;

import java.util.List;
import java.util.Map;

public class TableStorageDesc {
    private StorageSerDeInfo serDeInfo;
    private List<String> bucketCols;
    private boolean storedAsSubDirectories;
    private int numBuckets;
    private boolean compressed;
    private String outputFormat;
    private String inputFormat;
    private Map<String, String> parameters;
    private String location;

    public TableStorageDesc() {
    }

    public void setSerDeInfo(StorageSerDeInfo serDeInfo) {
        this.serDeInfo = serDeInfo;
    }

    public StorageSerDeInfo getSerDeInfo() {
        return this.serDeInfo;
    }

    public void setBucketCols(List<String> bucketCols) {
        this.bucketCols = bucketCols;
    }

    public List<String> getBucketCols() {
        return this.bucketCols;
    }

    public void setStoredAsSubDirectories(boolean storedAsSubDirectories) {
        this.storedAsSubDirectories = storedAsSubDirectories;
    }

    public boolean isStoredAsSubDirectories() {
        return this.storedAsSubDirectories;
    }

    public void setNumBuckets(int numBuckets) {
        this.numBuckets = numBuckets;
    }

    public int getNumBuckets() {
        return this.numBuckets;
    }

    public void setCompressed(boolean compressed) {
        this.compressed = compressed;
    }

    public boolean isCompressed() {
        return this.compressed;
    }

    public void setOutputFormat(String outputFormat) {
        this.outputFormat = outputFormat;
    }

    public String getOutputFormat() {
        return this.outputFormat;
    }

    public void setInputFormat(String inputFormat) {
        this.inputFormat = inputFormat;
    }

    public String getInputFormat() {
        return this.inputFormat;
    }

    public void setParameters(Map<String, String> parameters) {
        this.parameters = parameters;
    }

    public Map<String, String> getParameters() {
        return this.parameters;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getLocation() {
        return this.location;
    }
}

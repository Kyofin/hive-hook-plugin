package org.data.meta.hive.model.lineage;

import java.util.List;

public class LineageHookInfo {
    private String database;
    private Long duration;
    private String engine;
    private String hash;
    private List<String> jobIds;
    private String queryText;
    private Long timestamp;
    private String user;
    private String[] userGroupNames;
    private String version;
    private List<ColumnLineage> columnLineages;
    private List<TableLineage> tableLineages;
    private String conf;

    public LineageHookInfo() {
    }

    public String getDatabase() {
        return this.database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public Long getDuration() {
        return this.duration;
    }

    public void setDuration(Long duration) {
        this.duration = duration;
    }

    public String getEngine() {
        return this.engine;
    }

    public void setEngine(String engine) {
        this.engine = engine;
    }

    public String getHash() {
        return this.hash;
    }

    public void setHash(String hash) {
        this.hash = hash;
    }

    public List<String> getJobIds() {
        return this.jobIds;
    }

    public void setJobIds(List<String> jobIds) {
        this.jobIds = jobIds;
    }

    public String getQueryText() {
        return this.queryText;
    }

    public void setQueryText(String queryText) {
        this.queryText = queryText;
    }

    public Long getTimestamp() {
        return this.timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getUser() {
        return this.user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String[] getUserGroupNames() {
        return this.userGroupNames;
    }

    public void setUserGroupNames(String[] userGroupNames) {
        this.userGroupNames = userGroupNames;
    }

    public String getVersion() {
        return this.version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public List<ColumnLineage> getColumnLineages() {
        return this.columnLineages;
    }

    public void setColumnLineages(List<ColumnLineage> columnLineages) {
        this.columnLineages = columnLineages;
    }

    public List<TableLineage> getTableLineages() {
        return this.tableLineages;
    }

    public void setTableLineages(List<TableLineage> tableLineages) {
        this.tableLineages = tableLineages;
    }

    public String getConf() {
        return this.conf;
    }

    public void setConf(String conf) {
        this.conf = conf;
    }
}

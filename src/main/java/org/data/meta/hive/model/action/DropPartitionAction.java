package org.data.meta.hive.model.action;

import java.util.ArrayList;
import java.util.List;

public class DropPartitionAction extends HiveMetaAction {
    private String databaseName;
    private String tableName;
    private List<String> partitions = new ArrayList<>();

    public DropPartitionAction() {
        super("PARTITION", "DROPPARTITION");
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getDatabaseName() {
        return this.databaseName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTableName() {
        return this.tableName;
    }

    public List<String> getPartitions() {
        return this.partitions;
    }

    public void setPartitions(List<String> partitions) {
        this.partitions = partitions;
    }
}

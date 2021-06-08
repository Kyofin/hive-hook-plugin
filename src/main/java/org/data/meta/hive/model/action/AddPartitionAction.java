package org.data.meta.hive.model.action;

import org.data.meta.hive.model.PartitionInfo;

import java.util.ArrayList;
import java.util.List;

public class AddPartitionAction extends HiveMetaAction {
    private String databaseName;
    private String tableName;
    private List<PartitionInfo> partitions = new ArrayList<>();

    public AddPartitionAction() {
        super("PARTITION", "ADDPARTITION");
    }

    public AddPartitionAction(String objectType, String operationName) {
        super(objectType, operationName);
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

    public List<PartitionInfo> getPartitions() {
        return this.partitions;
    }

    public void setPartitions(List<PartitionInfo> partitions) {
        this.partitions = partitions;
    }
}

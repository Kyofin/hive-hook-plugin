package org.data.meta.hive.model.action;

import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.data.meta.hive.model.FieldSchema;
import org.data.meta.hive.model.TableStorageDesc;

public class CreateTableAction extends HiveMetaAction {
    private List<FieldSchema> columns;
    private List<FieldSchema> partitionKeys;
    private TableStorageDesc storageDesc;
    private boolean temporary;
    private String tableType;
    private Map<String, String> parameters;
    private int retention;
    private long lastAccessTime;
    private long createTime;
    private String owner;
    private String tableName;
    private String dataBaseName;

    public CreateTableAction() {
        super("TABLE", HiveOperation.CREATETABLE.getOperationName());
    }

    CreateTableAction(String objectType, String operationName) {
        super(objectType, operationName);
    }

    public void setColumns(List<FieldSchema> columns) {
        this.columns = columns;
    }

    public List<FieldSchema> getColumns() {
        return this.columns;
    }

    public List<FieldSchema> getPartitionKeys() {
        return this.partitionKeys;
    }

    public void setPartitionKeys(List<FieldSchema> partitionKeys) {
        this.partitionKeys = partitionKeys;
    }

    public void setStorageDesc(TableStorageDesc storageDesc) {
        this.storageDesc = storageDesc;
    }

    public TableStorageDesc getStorageDesc() {
        return this.storageDesc;
    }

    public void setTemporary(boolean temporary) {
        this.temporary = temporary;
    }

    public boolean isTemporary() {
        return this.temporary;
    }

    public void setTableType(String tableType) {
        this.tableType = tableType;
    }

    public String getTableType() {
        return this.tableType;
    }

    public void setParameters(Map<String, String> parameters) {
        this.parameters = parameters;
    }

    public Map<String, String> getParameters() {
        return this.parameters;
    }

    public void setRetention(int retention) {
        this.retention = retention;
    }

    public int getRetention() {
        return this.retention;
    }

    public void setLastAccessTime(long lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
    }

    public long getLastAccessTime() {
        return this.lastAccessTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public long getCreateTime() {
        return this.createTime;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getOwner() {
        return this.owner;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTableName() {
        return this.tableName;
    }

    public void setDataBaseName(String dataBaseName) {
        this.dataBaseName = dataBaseName;
    }

    public String getDataBaseName() {
        return this.dataBaseName;
    }
}

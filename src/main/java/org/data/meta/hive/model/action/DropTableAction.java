package org.data.meta.hive.model.action;

import org.apache.hadoop.hive.ql.plan.HiveOperation;

public class DropTableAction extends HiveMetaAction {
    private String tableName;
    private String databaseName;

    public DropTableAction() {
        super("TABLE", HiveOperation.DROPTABLE.getOperationName());
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTableName() {
        return this.tableName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getDatabaseName() {
        return this.databaseName;
    }
}

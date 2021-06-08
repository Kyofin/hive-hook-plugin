package org.data.meta.hive.model.action;

import org.apache.hadoop.hive.ql.plan.HiveOperation;

public class DropDataBaseAction extends HiveMetaAction {
    private String dataBaseName;

    public DropDataBaseAction() {
        super("DATABASE", HiveOperation.DROPDATABASE.getOperationName());
    }

    public void setDataBaseName(String dataBaseName) {
        this.dataBaseName = dataBaseName;
    }

    public String getDataBaseName() {
        return this.dataBaseName;
    }
}

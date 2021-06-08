package org.data.meta.hive.model.action;
import org.apache.hadoop.hive.metastore.api.Table;

public class AlterTableAction extends CreateTableAction {
    private Table newTable;
    private Table oldTable;

    public Table getNewTable() {
        return this.newTable;
    }

    public void setNewTable(Table newTable) {
        this.newTable = newTable;
    }

    public Table getOldTable() {
        return this.oldTable;
    }

    public void setOldTable(Table oldTable) {
        this.oldTable = oldTable;
    }

    public AlterTableAction() {
        super("TABLE", "ALTERTABLE");
    }
}

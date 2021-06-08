package org.data.meta.hive.model.lineage;

import java.util.Objects;

public class TableLineage {
    private String srcDatabase;
    private String destDatabase;
    private String srcTable;
    private String destTable;

    public TableLineage() {
    }

    @Override
    public int hashCode() {
        return Objects.hash(new Object[]{this.srcDatabase, this.destDatabase, this.srcTable, this.destTable});
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            TableLineage that = (TableLineage)o;
            return Objects.equals(this.srcDatabase, that.srcDatabase) && Objects.equals(this.destDatabase, that.destDatabase) && Objects.equals(this.srcTable, that.srcTable) && Objects.equals(this.destTable, that.destTable);
        } else {
            return false;
        }
    }

    public String getSrcDatabase() {
        return this.srcDatabase;
    }

    public void setSrcDatabase(String srcDatabase) {
        this.srcDatabase = srcDatabase;
    }

    public String getDestDatabase() {
        return this.destDatabase;
    }

    public void setDestDatabase(String destDatabase) {
        this.destDatabase = destDatabase;
    }

    public String getSrcTable() {
        return this.srcTable;
    }

    public void setSrcTable(String srcTable) {
        this.srcTable = srcTable;
    }

    public String getDestTable() {
        return this.destTable;
    }

    public void setDestTable(String destTable) {
        this.destTable = destTable;
    }
}

package org.data.meta.hive.model.lineage;

public class LineageTableColumn {
    private String table;
    private String column;

    public LineageTableColumn() {
    }

    public LineageTableColumn(String table, String column) {
        this.table = table;
        this.column = column;
    }

    public String getTable() {
        return this.table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getColumn() {
        return this.column;
    }

    public void setColumn(String column) {
        this.column = column;
    }
}

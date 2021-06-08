package org.data.meta.hive.model.lineage;

public class LineageTable {
    private String database;
    private String table;

    public LineageTable() {
    }

    public LineageTable(String database, String table) {
        this.database = database;
        this.table = table;
    }

    public String getDatabase() {
        return this.database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return this.table;
    }

    public void setTable(String table) {
        this.table = table;
    }
}

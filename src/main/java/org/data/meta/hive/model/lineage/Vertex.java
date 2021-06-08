package org.data.meta.hive.model.lineage;

public class Vertex {
    public int id;
    public Vertex.Type type;
    public String label;
    public String dbName;
    public String tableName;
    public String columnName;

    public Vertex(String label, Vertex.Type type, String dbName, String tableName, String columnName) {
        this.label = label;
        this.type = type;
        this.dbName = dbName;
        this.tableName = tableName;
        this.columnName = columnName;
    }

    @Override
    public int hashCode() {
        return this.label.hashCode() + this.type.hashCode() * 3;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (!(obj instanceof Vertex)) {
            return false;
        } else {
            Vertex vertex = (Vertex)obj;
            return this.label.equals(vertex.label) && this.type == vertex.type;
        }
    }

    public static enum Type {
        COLUMN,
        TABLE;

        private Type() {
        }
    }
}

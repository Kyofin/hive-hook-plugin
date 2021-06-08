package org.data.meta.hive.model.lineage;

import java.util.List;

public class ColumnLineage {
    private String srcDatabase;
    private String destDatabase;
    private String expression;
    private Edge.Type edgeType;
    private List<LineageTableColumn> sources;
    private List<LineageTableColumn> targets;

    public ColumnLineage() {
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

    public String getExpression() {
        return this.expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

    public Edge.Type getEdgeType() {
        return this.edgeType;
    }

    public void setEdgeType(Edge.Type edgeType) {
        this.edgeType = edgeType;
    }

    public List<LineageTableColumn> getSources() {
        return this.sources;
    }

    public void setSources(List<LineageTableColumn> sources) {
        this.sources = sources;
    }

    public List<LineageTableColumn> getTargets() {
        return this.targets;
    }

    public void setTargets(List<LineageTableColumn> targets) {
        this.targets = targets;
    }
}

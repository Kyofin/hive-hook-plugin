package org.data.meta.hive.model.lineage;

import java.util.Set;

public class Edge {
    public Set<Vertex> sources;
    public Set<Vertex> targets;
    public String expr;
    public Edge.Type type;

    public Edge(Set<Vertex> sources, Set<Vertex> targets, String expr, Edge.Type type) {
        this.sources = sources;
        this.targets = targets;
        this.expr = expr;
        this.type = type;
    }

    public static enum Type {
        PROJECTION,
        PREDICATE;

        private Type() {
        }
    }
}

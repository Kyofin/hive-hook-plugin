package org.data.meta.hive.model.audit;

import org.data.meta.hive.util.MetaLogUtils;

import java.util.List;
import java.util.Objects;

public class AuditLog {
    private String operation;
    private String user;
    private List<String> groups;
    private String db;
    private String tbl;
    private List<String> parts;
    private String oldDb;
    private String oldTbl;
    private List<String> oldParts;

    public AuditLog() {
    }

    public AuditLog(String operation, String user, List<String> groups, String db, String tbl, List<String> parts) {
        this.operation = operation;
        this.user = user;
        this.groups = groups;
        this.db = MetaLogUtils.normalizeIdentifier(db);
        this.tbl = MetaLogUtils.normalizeIdentifier(tbl);
        this.parts = parts;
    }

    public AuditLog(String operation, String user, List<String> groups, String db, String tbl, List<String> parts, String oldDb, String oldTbl, List<String> oldParts) {
        this.operation = operation;
        this.user = user;
        this.groups = groups;
        this.db = MetaLogUtils.normalizeIdentifier(db);
        this.tbl = MetaLogUtils.normalizeIdentifier(tbl);
        this.parts = parts;
        this.oldDb = oldDb;
        this.oldTbl = oldTbl;
        this.oldParts = oldParts;
    }

    @Override
    public int hashCode() {
        int prime = 3;
        int allHashCode = prime * this.hashIfNotNull(this.operation) + prime * this.hashIfNotNull(this.user) + prime * this.hashIfNotNull(this.groups) + prime * this.hashIfNotNull(this.db) + prime * this.hashIfNotNull(this.tbl) + prime * this.hashIfNotNull(this.parts) + prime * this.hashIfNotNull(this.oldDb) + prime * this.hashIfNotNull(this.oldTbl) + prime * this.hashIfNotNull(this.oldParts);
        return allHashCode + 37;
    }

    private int hashIfNotNull(Object o) {
        return o == null ? 0 : o.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof AuditLog)) {
            return false;
        } else {
            AuditLog another = (AuditLog)o;
            return Objects.equals(another.getOperation(), this.operation) && Objects.equals(another.getUser(), this.user) && Objects.equals(another.getGroups(), this.groups) && Objects.equals(another.getDb(), this.db) && Objects.equals(another.getTbl(), this.tbl) && Objects.equals(another.getParts(), this.parts) && Objects.equals(another.getOldDb(), this.oldDb) && Objects.equals(another.getOldTbl(), this.oldTbl) && Objects.equals(another.getOldParts(), this.oldParts);
        }
    }

    public String getOperation() {
        return this.operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public String getUser() {
        return this.user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public List<String> getGroups() {
        return this.groups;
    }

    public void setGroups(List<String> groups) {
        this.groups = groups;
    }

    public String getDb() {
        return this.db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getTbl() {
        return this.tbl;
    }

    public void setTbl(String tbl) {
        this.tbl = tbl;
    }

    public List<String> getParts() {
        return this.parts;
    }

    public void setParts(List<String> parts) {
        this.parts = parts;
    }

    public String getOldDb() {
        return this.oldDb;
    }

    public void setOldDb(String oldDb) {
        this.oldDb = oldDb;
    }

    public String getOldTbl() {
        return this.oldTbl;
    }

    public void setOldTbl(String oldTbl) {
        this.oldTbl = oldTbl;
    }

    public List<String> getOldParts() {
        return this.oldParts;
    }

    public void setOldParts(List<String> oldParts) {
        this.oldParts = oldParts;
    }
}

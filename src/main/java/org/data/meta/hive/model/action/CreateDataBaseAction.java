package org.data.meta.hive.model.action;

import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.ql.plan.HiveOperation;

import java.util.Map;

public class CreateDataBaseAction extends HiveMetaAction {
    private String dataBaseName;
    private String description;
    private String owner;
    private PrincipalType ownerType;
    private String location;
    private Map<String, String> parameters;

    public CreateDataBaseAction() {
        super("DATABASE", HiveOperation.CREATEDATABASE.getOperationName());
    }

    CreateDataBaseAction(String objectType, String operationName) {
        super(objectType, operationName);
    }

    public void setDataBaseName(String dataBaseName) {
        this.dataBaseName = dataBaseName;
    }

    public String getDataBaseName() {
        return this.dataBaseName;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getDescription() {
        return this.description;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getOwner() {
        return this.owner;
    }

    public void setOwnerType(PrincipalType ownerType) {
        this.ownerType = ownerType;
    }

    public PrincipalType getOwnerType() {
        return this.ownerType;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getLocation() {
        return this.location;
    }

    public void setParameters(Map<String, String> parameters) {
        this.parameters = parameters;
    }

    public Map<String, String> getParameters() {
        return this.parameters;
    }
}

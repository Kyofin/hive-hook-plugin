package org.data.meta.hive.model;

import java.util.Map;

public class StorageSerDeInfo {
    private Map<String, String> parameters;
    private String serializationLib;
    private String name;

    public StorageSerDeInfo() {
    }

    public void setParameters(Map<String, String> parameters) {
        this.parameters = parameters;
    }

    public Map<String, String> getParameters() {
        return this.parameters;
    }

    public void setSerializationLib(String serializationLib) {
        this.serializationLib = serializationLib;
    }

    public String getSerializationLib() {
        return this.serializationLib;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }
}

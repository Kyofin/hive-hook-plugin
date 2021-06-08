package org.data.meta.hive.listener;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.data.meta.hive.model.PartitionInfo;
import org.data.meta.hive.model.StorageSerDeInfo;
import org.data.meta.hive.model.TableStorageDesc;
import org.data.meta.hive.model.action.AddPartitionAction;
import org.data.meta.hive.model.action.AlterPartitionAction;
import org.data.meta.hive.model.action.AlterTableAction;
import org.data.meta.hive.model.action.CreateDataBaseAction;
import org.data.meta.hive.model.action.CreateTableAction;
import org.data.meta.hive.model.action.DropDataBaseAction;
import org.data.meta.hive.model.action.DropPartitionAction;
import org.data.meta.hive.model.action.DropTableAction;
import org.data.meta.hive.model.action.HiveMetaAction;
import org.data.meta.hive.model.event.HiveMetaEvent;
import org.data.meta.hive.service.emitter.EventEmitterFactory;
import org.data.meta.hive.util.HdfsUtils;
import org.data.meta.hive.util.JsonUtils;
import org.data.meta.hive.util.MetaLogUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetaStoreListener extends MetaStoreEventListener {
    private static final Logger LOG = LoggerFactory.getLogger(MetaStoreListener.class);

    public MetaStoreListener(Configuration config) {
        super(config);
    }

    @Override
    public void onCreateTable(CreateTableEvent tableEvent) throws MetaException {
        if (tableEvent.getStatus()) {
            Table table = this.normalizeTable(tableEvent.getTable());
            CreateTableAction createTableAction = new CreateTableAction();
            this.buildCreateTableAction(table, createTableAction);
            this.emitAction(createTableAction);
        }
    }

    @Override
    public void onDropTable(DropTableEvent tableEvent) throws MetaException {
        if (tableEvent.getStatus()) {
            Table table = this.normalizeTable(tableEvent.getTable());
            DropTableAction dropTableAction = new DropTableAction();
            dropTableAction.setDatabaseName(table.getDbName());
            dropTableAction.setTableName(table.getTableName());
            this.emitAction(dropTableAction);
        }
    }

    @Override
    public void onAlterTable(AlterTableEvent tableEvent) throws MetaException {
        if (tableEvent.getStatus()) {
            Table oldTable = this.normalizeTable(tableEvent.getOldTable());
            Table table = this.normalizeTable(tableEvent.getNewTable());
            AlterTableAction alterTableAction = new AlterTableAction();
            this.buildAlterTableAction(oldTable, table, alterTableAction);
            this.emitAction(alterTableAction);
        }
    }

    @Override
    public void onAddPartition(AddPartitionEvent partitionEvent) throws MetaException {
        if (partitionEvent.getStatus()) {
            Iterator<Partition> iterator = partitionEvent.getPartitionIterator();
            Table table = this.normalizeTable(partitionEvent.getTable());
            Configuration conf = partitionEvent.getHandler().getConf();
            List<PartitionInfo> partitionInfos = new ArrayList<>();

            try {
                while(iterator.hasNext()) {
                    Partition partition = (Partition)iterator.next();
                    PartitionInfo partitionInfo = this.buildPartitionInfo(partition, table, conf, false);
                    if (partitionInfo != null) {
                        partitionInfos.add(partitionInfo);
                    }
                }
            } catch (Exception var9) {
                LOG.error("onAddPartition failed", var9);
            }

            if (partitionInfos.size() != 0) {
                String databaseName = table.getDbName();
                String tableName = table.getTableName();
                AddPartitionAction addPartitionAction = new AddPartitionAction();
                addPartitionAction.setDatabaseName(databaseName);
                addPartitionAction.setTableName(tableName);
                addPartitionAction.setPartitions(partitionInfos);
                this.emitAction(addPartitionAction);
            }
        }
    }

    @Override
    public void onDropPartition(DropPartitionEvent partitionEvent) throws MetaException {
        if (partitionEvent.getStatus()) {
            Table table = this.normalizeTable(partitionEvent.getTable());
            List<String> partitions = new ArrayList<>();
            Partition partition = partitionEvent.getPartition();
            String dropPartName = this.buildDropPartitionName(partition, table);
            if (StringUtils.isNotEmpty(dropPartName)) {
                partitions.add(dropPartName);
            }

            if (partitions.size() != 0) {
                String databaseName = table.getDbName();
                String tableName = table.getTableName();
                DropPartitionAction dropPartitionAction = new DropPartitionAction();
                dropPartitionAction.setDatabaseName(databaseName);
                dropPartitionAction.setTableName(tableName);
                dropPartitionAction.setPartitions(partitions);
                this.emitAction(dropPartitionAction);
            }
        }
    }

    @Override
    public void onAlterPartition(AlterPartitionEvent partitionEvent) throws MetaException {
        if (partitionEvent.getStatus()) {
            Table table = this.normalizeTable(partitionEvent.getTable());
            Partition newPartition = partitionEvent.getNewPartition();
            Partition oldPartition = partitionEvent.getOldPartition();
            Configuration conf = partitionEvent.getHandler().getConf();
            List<PartitionInfo> partitionInfos = new ArrayList<>();
            PartitionInfo oldPartitionInfo = null;

            try {
                PartitionInfo partitionInfo = this.buildPartitionInfo(newPartition, table, conf, false);
                if (partitionInfo != null) {
                    partitionInfos.add(partitionInfo);
                }

                oldPartitionInfo = this.buildPartitionInfo(oldPartition, table, conf, true);
            } catch (Exception var11) {
                LOG.error("onAlterPartition failed", var11);
            }

            if (partitionInfos.size() != 0) {
                String databaseName = table.getDbName();
                String tableName = table.getTableName();
                AlterPartitionAction alterPartitionAction = new AlterPartitionAction();
                alterPartitionAction.setDatabaseName(databaseName);
                alterPartitionAction.setTableName(tableName);
                alterPartitionAction.setPartitions(partitionInfos);
                alterPartitionAction.setOldPartition(oldPartitionInfo);
                this.emitAction(alterPartitionAction);
            }
        }
    }

    @Override
    public void onCreateDatabase(CreateDatabaseEvent dbEvent) throws MetaException {
        if (dbEvent.getStatus()) {
            Database database = this.normalizeDatabase(dbEvent.getDatabase());
            String databaseName = database.getName();
            String description = database.getDescription();
            String owner = database.getOwnerName();
            PrincipalType ownerType = database.getOwnerType();
            String location = database.getLocationUri();
            Map<String, String> parameters = database.getParameters();
            CreateDataBaseAction createDataBaseAction = new CreateDataBaseAction();
            createDataBaseAction.setDataBaseName(databaseName);
            createDataBaseAction.setDescription(description);
            createDataBaseAction.setOwner(owner);
            createDataBaseAction.setOwnerType(ownerType);
            createDataBaseAction.setLocation(location);
            createDataBaseAction.setParameters(parameters);
            this.emitAction(createDataBaseAction);
        }
    }

    @Override
    public void onDropDatabase(DropDatabaseEvent dbEvent) throws MetaException {
        if (dbEvent.getStatus()) {
            Database database = this.normalizeDatabase(dbEvent.getDatabase());
            DropDataBaseAction dropDataBaseAction = new DropDataBaseAction();
            dropDataBaseAction.setDataBaseName(database.getName());
            this.emitAction(dropDataBaseAction);
        }
    }

    private void buildCreateTableAction(Table table, CreateTableAction createTableAction) {
        StorageDescriptor sd = table.getSd();
        List<FieldSchema> cols = sd.getCols();
        String tableName = table.getTableName();
        String dbName = table.getDbName();
        String owner = table.getOwner();
        boolean temporary = table.isTemporary();
        List<FieldSchema> partitionKeys = table.getPartitionKeys();
        String tableType = table.getTableType();
        int createTime = table.getCreateTime();
        int retention = table.getRetention();
        int lastAccessTime = table.getLastAccessTime();
        Map<String, String> parameters = table.getParameters();
        createTableAction.setColumns(getColumns(cols));
        createTableAction.setTemporary(temporary);
        createTableAction.setPartitionKeys(getColumns(partitionKeys));
        createTableAction.setStorageDesc(getStorageDesc(table));
        createTableAction.setTableType(tableType);
        createTableAction.setParameters(parameters);
        createTableAction.setRetention(retention);
        createTableAction.setLastAccessTime((long)lastAccessTime);
        createTableAction.setCreateTime((long)createTime);
        createTableAction.setOwner(owner);
        createTableAction.setTableName(tableName);
        createTableAction.setDataBaseName(dbName);
    }

    public static List<org.data.meta.hive.model.FieldSchema> getColumns(List<FieldSchema> fields) {
        List<org.data.meta.hive.model.FieldSchema> fieldSchemas = new ArrayList<>();
        int position = 0;

        for (FieldSchema field : fields) {
            org.data.meta.hive.model.FieldSchema fieldSchema = new org.data.meta.hive.model.FieldSchema();
            fieldSchema.setColumnName(field.getName());
            fieldSchema.setType(field.getType());
            fieldSchema.setPosition(position++);
            fieldSchema.setComment(field.getComment());
            fieldSchemas.add(fieldSchema);
        }

        return fieldSchemas;
    }

    private static TableStorageDesc getStorageDesc(Table table) {
        TableStorageDesc tableStorageDesc = new TableStorageDesc();
        StorageDescriptor storageDesc = table.getSd();
        tableStorageDesc.setLocation(storageDesc.getLocation());
        tableStorageDesc.setParameters(storageDesc.getParameters());
        tableStorageDesc.setInputFormat(storageDesc.getInputFormat());
        tableStorageDesc.setOutputFormat(storageDesc.getOutputFormat());
        tableStorageDesc.setCompressed(storageDesc.isCompressed());
        tableStorageDesc.setNumBuckets(storageDesc.getNumBuckets());
        tableStorageDesc.setStoredAsSubDirectories(storageDesc.isSetStoredAsSubDirectories());
        tableStorageDesc.setBucketCols(storageDesc.getBucketCols());
        SerDeInfo serDeInfo = storageDesc.getSerdeInfo();
        if (serDeInfo != null) {
            StorageSerDeInfo storageSerDeInfo = new StorageSerDeInfo();
            storageSerDeInfo.setName(serDeInfo.getName());
            storageSerDeInfo.setSerializationLib(serDeInfo.getSerializationLib());
            storageSerDeInfo.setParameters(serDeInfo.getParameters());
            tableStorageDesc.setSerDeInfo(storageSerDeInfo);
        }

        return tableStorageDesc;
    }

    private void buildAlterTableAction(Table oldTable, Table newTable, AlterTableAction action) {
        this.buildCreateTableAction(newTable, action);
        action.setOldTable(oldTable);
        action.setNewTable(newTable);
    }

    private PartitionInfo buildPartitionInfo(Partition partition, Table table, Configuration conf, boolean omitResolvingPath) throws IOException {
        List<String> partitionValues = partition.getValues();
        if (CollectionUtils.isEmpty(partitionValues)) {
            return null;
        } else {
            List<FieldSchema> partitionKeys = table.getPartitionKeys();
            if (partitionKeys != null && partitionKeys.size() != 0) {
                if (partitionKeys.size() != partitionValues.size()) {
                    LOG.error("find table partitionKeys not equals to partition event value size, table : {}.{}, , partitionKeys : {}, partition : {}", new Object[]{table.getDbName(), table.getTableName(), table.getPartitionKeys(), JsonUtils.toJsonString(partition)});
                }

                String partitionName = MetaLogUtils.getPartitionName(partitionKeys, partitionValues);
                PartitionInfo partitionInfo = new PartitionInfo();
                partitionInfo.setPartitionName(partitionName);
                partitionInfo.setCreateTime((long)partition.getCreateTime());
                partitionInfo.setLastAccessTime((long)partition.getLastAccessTime());
                partitionInfo.setBucketNum(partition.getSd().getNumBuckets());
                if (!omitResolvingPath) {
                    Path path = this.getDataLocation(table, partition);
                    if (path != null) {
                        partitionInfo.setPartitionPath(HdfsUtils.resolvePath(conf, path));
                        partitionInfo.setDataLocation(HdfsUtils.resolvePath(conf, path));
                    }
                }

                return partitionInfo;
            } else {
                LOG.error("find table partitionKeys empty, table : {}.{}, partition : {}", new Object[]{table.getDbName(), table.getTableName(), JsonUtils.toJsonString(partition)});
                return null;
            }
        }
    }

    private String buildDropPartitionName(Partition partition, Table table) {
        List<FieldSchema> partitionKeys = table.getPartitionKeys();
        if (partitionKeys != null && partitionKeys.size() != 0) {
            if (partitionKeys.size() != partition.getValues().size()) {
                LOG.error("buildDropPartitionNames find table partitionKeys not equals to partition event value size, table : {}.{}, , partitionKeys : {}, partition : {}", new Object[]{table.getDbName(), table.getTableName(), table.getPartitionKeys(), JsonUtils.toJsonString(partition)});
            }

            List<String> partitionKeyValues = new ArrayList<>();

            for(int i = 0; i < partitionKeys.size(); ++i) {
                partitionKeyValues.add(((FieldSchema)partitionKeys.get(i)).getName() + "=" + (String)partition.getValues().get(i));
            }

            return StringUtils.join(partitionKeyValues, "/");
        } else {
            LOG.error("buildDropPartitionNames find table partitionKeys empty, table : {}.{}, partition : {}", new Object[]{table.getDbName(), table.getTableName(), JsonUtils.toJsonString(partition)});
            return null;
        }
    }

    private Path getDataLocation(Table table, Partition partition) {
        if (table.isSetPartitionKeys()) {
            return partition.getSd() != null && StringUtils.isNotEmpty(partition.getSd().getLocation()) ? new Path(partition.getSd().getLocation()) : null;
        } else {
            return table.getSd() != null && StringUtils.isNotEmpty(table.getSd().getLocation()) ? new Path(table.getSd().getLocation()) : null;
        }
    }

    private void emitAction(HiveMetaAction action) {
        if (action == null) {
            LOG.error("emitAction failed, action is null");
        }

        HiveMetaEvent metaEvent = HiveMetaEvent.of(action);

        try {
            EventEmitterFactory.get().emit(metaEvent);
        } catch (IOException var4) {
            LOG.error("EventEmitter emit failed", var4);
        }

    }

    private Database normalizeDatabase(Database database) {
        database.setName(MetaLogUtils.normalizeIdentifier(database.getName()));
        return database;
    }

    private Table normalizeTable(Table table) {
        table.setDbName(MetaLogUtils.normalizeIdentifier(table.getDbName()));
        table.setTableName(MetaLogUtils.normalizeIdentifier(table.getTableName()));
        return table;
    }
}

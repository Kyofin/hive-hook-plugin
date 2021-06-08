package org.data.meta.hive.listener;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import javax.security.auth.login.LoginException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStorePreEventListener;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.PreAddIndexEvent;
import org.apache.hadoop.hive.metastore.events.PreAddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.PreAlterIndexEvent;
import org.apache.hadoop.hive.metastore.events.PreAlterTableEvent;
import org.apache.hadoop.hive.metastore.events.PreCreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.PreCreateTableEvent;
import org.apache.hadoop.hive.metastore.events.PreDropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.PreDropIndexEvent;
import org.apache.hadoop.hive.metastore.events.PreDropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.PreDropTableEvent;
import org.apache.hadoop.hive.metastore.events.PreEventContext;
import org.apache.hadoop.hive.metastore.events.PreReadDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.PreReadTableEvent;
import org.apache.hadoop.hive.metastore.events.PreEventContext.PreEventType;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.UserGroupInformation;
import org.data.meta.hive.model.audit.AuditLog;
import org.data.meta.hive.model.event.EventBase;
import org.data.meta.hive.service.emitter.EventEmitterFactory;
import org.data.meta.hive.util.EventUtils;
import org.data.meta.hive.util.JsonUtils;
import org.data.meta.hive.util.MetaLogUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetaStorePreAuditListener extends MetaStorePreEventListener {
    private static final Logger LOG = LoggerFactory.getLogger(MetaStorePreAuditListener.class);

    public MetaStorePreAuditListener(Configuration config) {
        super(config);
    }

    @Override
    public void onEvent(PreEventContext preEventContext) throws MetaException, NoSuchObjectException, InvalidOperationException {
        String user = null;
        List groups = null;

        try {
            UserGroupInformation ugi = Utils.getUGI();
            user = ugi.getUserName();
            groups = Arrays.asList(ugi.getGroupNames());
        } catch (IOException | LoginException e) {
            LOG.error("getUGI failed. ", e);
        }

        try {
            PreEventType preEventType = preEventContext.getEventType();
            Index droppedIndex;
            String db;
            String tbl;
            AuditLog auditLog;
            Database database;
            Table table;
            switch(preEventType) {
            case CREATE_TABLE:
                PreCreateTableEvent preCreateTableEvent = (PreCreateTableEvent)preEventContext;
                table = preCreateTableEvent.getTable();
                db = table.getDbName();
                tbl = table.getTableName();
                auditLog = new AuditLog(preEventType.name(), user, groups, db, tbl, null);
                this.emitEvent(auditLog);
                break;
            case DROP_TABLE:
                PreDropTableEvent preDropTableEvent = (PreDropTableEvent)preEventContext;
                table = preDropTableEvent.getTable();
                db = table.getDbName();
                tbl = table.getTableName();
                auditLog = new AuditLog(preEventType.name(), user, groups, db, tbl, null);
                this.emitEvent(auditLog);
                break;
            case ALTER_TABLE:
                PreAlterTableEvent preAlterTableEvent = (PreAlterTableEvent)preEventContext;
                table = preAlterTableEvent.getOldTable();
                Table newTable = preAlterTableEvent.getNewTable();
                tbl = newTable.getDbName();
                tbl = newTable.getTableName();
                String oldDb = table.getDbName();
                String oldTbl = table.getTableName();
                 auditLog = new AuditLog(preEventType.name(), user, groups, tbl, tbl, null, oldDb, oldTbl, null);
                this.emitEvent(auditLog);
                break;
            case ADD_PARTITION:
                this.handlePreAddPartitionEvent(preEventContext, user, groups);
                break;
            case DROP_PARTITION:
                this.handleDropPartitionEvent(preEventContext, user, groups);
            case ALTER_PARTITION:

            case CREATE_DATABASE:
                PreCreateDatabaseEvent preCreateDatabaseEvent = (PreCreateDatabaseEvent)preEventContext;
                database = preCreateDatabaseEvent.getDatabase();
                db = database.getName();
                auditLog = new AuditLog(preEventType.name(), user, groups, db, null, null);
                this.emitEvent(auditLog);
                break;
            case DROP_DATABASE:
                PreDropDatabaseEvent preDropDatabaseEvent = (PreDropDatabaseEvent)preEventContext;
                database = preDropDatabaseEvent.getDatabase();
                db = database.getName();
                auditLog = new AuditLog(preEventType.name(), user, groups, db, null, null);
                this.emitEvent(auditLog);
                break;
            case READ_TABLE:
                PreReadTableEvent preReadTableEvent = (PreReadTableEvent)preEventContext;
                table = preReadTableEvent.getTable();
                db = table.getDbName();
                tbl = table.getTableName();
                auditLog = new AuditLog(preEventType.name(), user, groups, db, tbl, null);
                this.emitEvent(auditLog);
                break;
            case READ_DATABASE:
                PreReadDatabaseEvent preReadDatabaseEvent = (PreReadDatabaseEvent)preEventContext;
                database = preReadDatabaseEvent.getDatabase();
                db = database.getName();
                auditLog = new AuditLog(preEventType.name(), user, groups, db, null, null);
                this.emitEvent(auditLog);
                break;
            case ADD_INDEX:
                PreAddIndexEvent preAddIndexEvent = (PreAddIndexEvent)preEventContext;
                droppedIndex = preAddIndexEvent.getIndex();
                db = droppedIndex.getDbName();
                tbl = droppedIndex.getIndexTableName();
                auditLog = new AuditLog(preEventType.name(), user, groups, db, tbl, null);
                this.emitEvent(auditLog);
                break;
            case ALTER_INDEX:
                PreAlterIndexEvent preAlterIndexEvent = (PreAlterIndexEvent)preEventContext;
                droppedIndex = preAlterIndexEvent.getOldIndex();
                Index newIndex = preAlterIndexEvent.getNewIndex();
                tbl = newIndex.getDbName();
                tbl = newIndex.getIndexTableName();
                auditLog = new AuditLog(preEventType.name(), user, groups, tbl, tbl, null);
                this.emitEvent(auditLog);
                break;
            case DROP_INDEX:
                PreDropIndexEvent preDropIndexEvent = (PreDropIndexEvent)preEventContext;
                droppedIndex = preDropIndexEvent.getIndex();
                db = droppedIndex.getDbName();
                tbl = droppedIndex.getIndexTableName();
                auditLog = new AuditLog(preEventType.name(), user, groups, db, tbl, null);
                this.emitEvent(auditLog);
                break;
            default:
                break;
            }
        } catch (Exception e) {
            LOG.error("error in onEvent", e);
        }

    }

    private void handlePreAddPartitionEvent(PreEventContext preEventContext, String user, List<String> groups) throws IOException {
        PreAddPartitionEvent preAddPartitionEvent = (PreAddPartitionEvent)preEventContext;
        Table table = preAddPartitionEvent.getTable();
        List<Partition> partitions = preAddPartitionEvent.getPartitions();
        String db = table.getDbName();
        String tbl = table.getTableName();
        List<String> parts = new ArrayList<>();
        List<FieldSchema> partitionKeys = this.getPartitionKeys(table);
        if (partitionKeys != null) {

            for (Partition partition : partitions) {
                String partitionName = this.buildPartitionName(partition, table, partitionKeys);
                if (partitionName != null) {
                    parts.add(partitionName);
                }
            }

            AuditLog auditLog = new AuditLog(preEventContext.getEventType().name(), user, groups, db, tbl, parts, null, null, null);
            this.emitEvent(auditLog);
        }
    }

    private void handleDropPartitionEvent(PreEventContext preEventContext, String user, List<String> groups) throws IOException {
        PreDropPartitionEvent preDropPartitionEvent = (PreDropPartitionEvent)preEventContext;
        Table table = preDropPartitionEvent.getTable();
        String db = table.getDbName();
        String tbl = table.getTableName();
        List<String> parts = new ArrayList<>();
        List<FieldSchema> partitionKeys = this.getPartitionKeys(table);
        if (partitionKeys != null) {
            Partition partition = preDropPartitionEvent.getPartition();
            List<String> partitionValues = partition.getValues();
            if (partitionKeys.size() != partitionValues.size()) {
                LOG.error("find table partitionKeys do not equal to partition event value size, table : {}.{}, , partitionKeys : {}, partition : {}", new Object[]{table.getDbName(), table.getTableName(), table.getPartitionKeys(), JsonUtils.toJsonString(partition)});
            } else {
                String partitionName = MetaLogUtils.getPartitionName(partitionKeys, partitionValues);
                parts.add(partitionName);
            }

            AuditLog auditLog = new AuditLog(preEventContext.getEventType().name(), user, groups, db, tbl, parts, null, null, null);
            this.emitEvent(auditLog);
        }
    }

    private List<FieldSchema> getPartitionKeys(Table table) {
        List<FieldSchema> partitionKeys = table.getPartitionKeys();
        if (partitionKeys != null && partitionKeys.size() != 0) {
            return partitionKeys;
        } else {
            LOG.error("find table partitionKeys empty, table : {}.{}", table.getDbName(), table.getTableName());
            return null;
        }
    }

    private String buildPartitionName(Partition partition, Table table, List<FieldSchema> partitionKeys) {
        List<String> partitionValues = partition.getValues();
        if (partitionKeys.size() != partitionValues.size()) {
            LOG.error("find table partitionKeys do not equal to partition event value size, table : {}.{}, , partitionKeys : {}, partition : {}", new Object[]{table.getDbName(), table.getTableName(), table.getPartitionKeys(), JsonUtils.toJsonString(partition)});
            return null;
        } else {
            return MetaLogUtils.getPartitionName(partitionKeys, partitionValues);
        }
    }

    private void emitEvent(AuditLog auditLog) throws IOException {
        EventBase<AuditLog> event = new EventBase();
        event.setEventType("AUDIT");
        event.setContent(auditLog);
        event.setId(EventUtils.newId());
        event.setTimestamp(System.currentTimeMillis());
        event.setType("HIVE");
        EventEmitterFactory.get().emit(event);
    }
}
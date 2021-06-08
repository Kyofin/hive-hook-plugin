package org.data.meta.hive.model.action;

import org.data.meta.hive.model.PartitionInfo;

public class AlterPartitionAction extends AddPartitionAction {
    private PartitionInfo oldPartition;

    public PartitionInfo getOldPartition() {
        return this.oldPartition;
    }

    public void setOldPartition(PartitionInfo oldPartition) {
        this.oldPartition = oldPartition;
    }

    public AlterPartitionAction() {
        super("PARTITION", "ALTERPARTITION");
    }
}

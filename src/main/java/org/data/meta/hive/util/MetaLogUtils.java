package org.data.meta.hive.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hive.common.util.HiveStringUtils;

public class MetaLogUtils {
    public MetaLogUtils() {
    }

    public static String getPartitionName(List<FieldSchema> partitionKeys, List<String> partitionValues) {
        List<String> partitionColumns = new ArrayList<>();

        for (FieldSchema partitionKey : partitionKeys) {
            partitionColumns.add(partitionKey.getName());
        }

        return FileUtils.makePartName(partitionColumns, partitionValues);
    }

    public static String normalizeIdentifier(String identifier) {
        return StringUtils.isBlank(identifier) ? identifier : HiveStringUtils.normalizeIdentifier(identifier);
    }
}

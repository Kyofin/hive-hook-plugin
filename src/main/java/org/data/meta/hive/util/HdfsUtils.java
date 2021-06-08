package org.data.meta.hive.util;

import org.apache.hadoop.fs.FileSystem;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsUtils {
    public HdfsUtils() {
    }

    public static String resolvePath(Configuration conf, Path path) throws IOException {
        return FileSystem.get(path.toUri(), conf).resolvePath(path).toUri().toString();
    }
}

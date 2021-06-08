package org.data.meta.hive.util;

import java.util.UUID;

public class EventUtils {
    public EventUtils() {
    }

    public static String newId() {
        return "U-" + UUID.randomUUID().toString();
    }
}

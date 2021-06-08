package org.data.meta.hive.util;

import com.google.gson.Gson;

public class JsonUtils {
    private static final Gson GSON = new Gson();

    public JsonUtils() {
    }

    public static String toJsonString(Object o) {
        return GSON.toJson(o);
    }
}

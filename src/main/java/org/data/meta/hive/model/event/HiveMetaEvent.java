package org.data.meta.hive.model.event;

import org.data.meta.hive.model.action.HiveMetaAction;
import org.data.meta.hive.util.EventUtils;

public class HiveMetaEvent<T extends HiveMetaAction> extends EventBase<T> {
    public static <T extends HiveMetaAction> HiveMetaEvent<T> of(T content) {
        HiveMetaEvent<T> event = new HiveMetaEvent(EventUtils.newId(), "HIVE", System.currentTimeMillis());
        event.setContent(content);
        return event;
    }

    public HiveMetaEvent() {
    }

    private HiveMetaEvent(String id, String type, Long timestamp) {
        super(id, type, timestamp);
    }
}

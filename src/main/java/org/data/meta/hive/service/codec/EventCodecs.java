package org.data.meta.hive.service.codec;

import org.data.meta.hive.model.event.Event;

public class EventCodecs {
    private static final Codec CODEC = new JsonEventCodec();

    public EventCodecs() {
    }

    public static <T> byte[] encode(Event<T> event) {
        return CODEC.encode(event);
    }
}

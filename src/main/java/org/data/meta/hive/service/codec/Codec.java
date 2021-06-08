package org.data.meta.hive.service.codec;

import org.data.meta.hive.model.event.Event;

public interface Codec {
    <T> byte[] encode(Event<T> event);
}

package org.data.meta.hive.service.emitter;

import org.data.meta.hive.model.event.Event;

import java.io.IOException;

public interface EventEmitter {
    <T> void emit(Event<T> event) throws IOException;
}

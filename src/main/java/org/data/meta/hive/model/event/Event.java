package org.data.meta.hive.model.event;

public interface Event<T> {
    String getEventType();

    String getId();

    String getType();

    Long getTimestamp();

    T getContent();
}

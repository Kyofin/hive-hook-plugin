package org.data.meta.hive.model.event;

public class EventBase<T> implements Event<T> {
    private String eventType;
    private String id;
    private String type;
    private Long timestamp;
    private T content;

    public EventBase() {
    }

    public EventBase(String id, String type, Long timestamp) {
        this.id = id;
        this.type = type;
        this.timestamp = timestamp;
    }

    public String getEventType() {
        return this.eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getId() {
        return this.id;
    }

    public String getType() {
        return this.type;
    }

    public Long getTimestamp() {
        return this.timestamp;
    }

    public T getContent() {
        return this.content;
    }

    public void setContent(T content) {
        this.content = content;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
}

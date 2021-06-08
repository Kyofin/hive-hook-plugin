package org.data.meta.hive.service.codec;

import org.data.meta.hive.model.event.Event;
import org.data.meta.hive.util.JsonUtils;

import java.nio.charset.StandardCharsets;

public class JsonEventCodec implements Codec {
    public JsonEventCodec() {
    }

    @Override
    public <T> byte[] encode(Event<T> event) {
        if (event == null) {
            return null;
        } else {
            T content = event.getContent();
            return content == null ? ("EVENT##" + event.getId() + "##" + event.getTimestamp() + "##" + event.getType() + "####").getBytes(StandardCharsets.UTF_8) : ("EVENT##" + event.getId() + "##" + event.getTimestamp() + "##" + event.getType() + "##" + content.getClass().getSimpleName() + "##" + JsonUtils.toJsonString(content)).getBytes(StandardCharsets.UTF_8);
        }
    }
}

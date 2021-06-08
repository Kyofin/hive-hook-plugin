package org.data.meta.hive.service.emitter;

import org.data.meta.hive.service.emitter.impl.RollingFileEmitterImpl;

public class EventEmitterFactory {
    private static final EventEmitter SINGLETON_EMITTER;

    public EventEmitterFactory() {
    }

    public static EventEmitter get() {
        return SINGLETON_EMITTER;
    }

    static {
        RollingFileEmitterImpl emitter = null;

        try {
            emitter = new RollingFileEmitterImpl();
        } catch (Throwable var2) {
            var2.printStackTrace();
        }

        SINGLETON_EMITTER = emitter;
    }
}

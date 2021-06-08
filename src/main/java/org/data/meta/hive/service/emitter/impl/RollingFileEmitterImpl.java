package org.data.meta.hive.service.emitter.impl;

import java.io.IOException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.data.meta.hive.model.event.Event;
import org.data.meta.hive.service.codec.EventCodecs;
import org.data.meta.hive.service.emitter.EventEmitter;

public class RollingFileEmitterImpl implements EventEmitter {
    private static final RollingFileWriter rollingFileWriter = new RollingFileWriter(50000, "hook.event");

    public RollingFileEmitterImpl() {
    }

    public <T> void emit(Event<T> event) throws IOException {
        String message = new String(EventCodecs.encode(event));
        LogHelper console = SessionState.getConsole();
        if (console != null) {
            console.printInfo(message);
        }

        rollingFileWriter.writeLineWithLock(message);
    }
}

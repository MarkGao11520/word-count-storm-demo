package com.gwf.log.formatter;

import ch.qos.logback.classic.spi.ILoggingEvent;
import com.gwf.log.formatter.Formatter;

public class MessageFormatter implements Formatter {
    @Override
    public String format(ILoggingEvent event) {
        return event.getFormattedMessage();
    }
}

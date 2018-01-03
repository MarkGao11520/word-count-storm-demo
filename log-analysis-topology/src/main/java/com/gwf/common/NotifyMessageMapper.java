package com.gwf.common;

import storm.trident.tuple.TridentTuple;

import java.util.Date;

public class NotifyMessageMapper implements MessageMapper {
    private static final long serialVersionUID = 3290317455056695833L;

    @Override
    public String toMessageBody(TridentTuple tuple) {
        StringBuilder sb = new StringBuilder();
        sb
                .append("On ")
                .append(new Date(tuple.getLongByField("timestamp")))
                .append(" ")
                .append("the application \"")
                .append(tuple.getStringByField("logger"))
                .append("\"")
                .append("change alert state based on a threshold of ")
                .append(tuple.getDoubleByField("threshold"))
                .append(".\n")
                .append("The last value was ")
                .append(tuple.getDoubleByField("average"))
                .append("\n")
                .append("The last message was \"")
                .append(tuple.getStringByField("message"))
                .append("\"");
        return sb.toString();
    }
}

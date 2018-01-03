package com.gwf.common;

import storm.trident.tuple.TridentTuple;

import java.io.Serializable;

public interface MessageMapper extends Serializable{
    String toMessageBody(TridentTuple tuple);
}

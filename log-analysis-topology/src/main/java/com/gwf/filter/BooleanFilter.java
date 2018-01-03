package com.gwf.filter;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

public class BooleanFilter extends BaseFilter {
    private static final long serialVersionUID = 5739300866454911623L;

    @Override
    public boolean isKeep(TridentTuple tridentTuple) {
        return tridentTuple.getBoolean(0);
    }
}

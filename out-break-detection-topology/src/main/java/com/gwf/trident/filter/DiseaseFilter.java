package com.gwf.trident.filter;

import com.gwf.trident.event.DiagnosisEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

import java.util.Date;

/**
 * filter不能修改tuple的值，也不能添加删除
 */
public class DiseaseFilter extends BaseFilter {
    private static final long serialVersionUID = -1354031526420455269L;

    private static final Logger LOG = LoggerFactory.getLogger(DiseaseFilter.class);


    /**
     * 是否过滤
     * @param tridentTuple
     * @return
     */
    @Override
    public boolean isKeep(TridentTuple tridentTuple) {
//        System.out.println("isKeep->"+new Date());
        DiagnosisEvent diagnosis = (DiagnosisEvent) tridentTuple.getValue(0);
        Integer code = Integer.parseInt(diagnosis.getDiagnosisCode());
        if(code.intValue()<=322){
            LOG.debug("Emitting disease ["+diagnosis.getDiagnosisCode()+"]");
            return true;
        }else {
            LOG.debug("Filtering disease ["+diagnosis.getDiagnosisCode()+"]");
            return false;
        }
    }
}

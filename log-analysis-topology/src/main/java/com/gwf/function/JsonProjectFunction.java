package com.gwf.function;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.json.simple.JSONValue;
import storm.trident.Stream;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.Map;


public class JsonProjectFunction extends BaseFunction{
    private static final long serialVersionUID = 1153005714345027509L;

    private Fields fields;

    public JsonProjectFunction(Fields fields){
        this.fields = fields;
    }


    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        String json = tridentTuple.getString(0);
        Map<String,Object> map = (Map<String, Object>) JSONValue.parse(json);
        Values values = new Values();
        for(int i=0;i<this.fields.size();i++){
            values.add(map.get(this.fields.get(i)));
        }
        tridentCollector.emit(values);
    }
}

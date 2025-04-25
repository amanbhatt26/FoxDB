package org.foxdb.table;

import java.util.HashMap;
import java.util.Map;

public class Schema {
    public enum Type{
        INTEGER,STRING
    }
    private Map<String, Type> fields;

    public Schema(){
        fields = new HashMap<>();
    }
    public void add(String name, Type type){
        fields.put(name,type);
    }

    public void remove(String name){
        fields.remove(name);
    }

    public Type type(String name){
        return fields.get(name);
    }

    public boolean contains(String name){
        return fields.containsKey(name);
    }

    public String[] fields(){
        return fields.keySet().toArray(new String[0]);
    }

    public boolean equals(Schema other){
        String[] otherFields = other.fields();
        if(otherFields.length != fields().length) return false;
        for(var f:otherFields){
            if(!contains(f) || type(f) != other.type(f)) return false;
        }
        return true;
    }
}

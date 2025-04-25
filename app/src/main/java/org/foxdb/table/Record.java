package org.foxdb.table;

import org.foxdb.transaction.Transaction;

import java.util.HashMap;
import java.util.Map;

public class Record {
    private RecordID rID;
    private Schema schema;
    private Map<String,Integer> intFields;
    private Map<String,String> stringFields;
    private Layout layout;

    public Record(RecordID rId, Schema schema){
        this.rID = rId;
        this.schema = schema;
        intFields = new HashMap<>();
        stringFields = new HashMap<>();
        this.layout = new Layout(schema);
    }

    public String[] fields(){
        return schema.fields();
    }

    public boolean contains(String name){
        return schema.contains(name);
    }

    public Schema.Type type(String name){
        return schema.type(name);
    }

    public int readInt(String name){
        if(schema.contains(name) && schema.type(name) == Schema.Type.INTEGER){
            return intFields.getOrDefault(name, 0);
        }
        throw new RuntimeException("Int Field of name " + name + " not present");
    }

    public String readString(String name){
        if(schema.contains(name) && schema.type(name) == Schema.Type.STRING){
            return stringFields.getOrDefault(name, null);
        }
        throw new RuntimeException("String Field of name " + name + " not present");
    }

    public void updateInt(String name, int val){
        if(schema.contains(name) && schema.type(name) == Schema.Type.INTEGER) {
            intFields.put(name,val);
        }
        throw new RuntimeException("Int Field of name " + name + " not present");
    }

    public void updateString(String name,String val){
        if(schema.contains(name) && schema.type(name) == Schema.Type.STRING){
            stringFields.put(name,val);
        }
        throw new RuntimeException("String Field of name " + name + " not present");
    }
}

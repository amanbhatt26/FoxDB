package org.foxdb.table;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class Layout {

    private Schema schema;
    private Map<String, Integer> offsets;
    private Map<String, Integer> sizes;

    public int recordSize(){
        int size = 0;
        for(int k:sizes.values()){
            size += k;
        }
        return size;
    }
    public int offset(String name){
        return offsets.get(name);
    }

    public int size(String name){
        return sizes.get(name);
    }
    public Layout(Schema schema){
        this.schema = schema;
        offsets = new HashMap<>();
        sizes = new HashMap<>();
        calculateOffsets();
    }

    public void updateSchema(Schema newSchema){
        this.schema = newSchema;
        offsets = new HashMap<>();
        sizes = new HashMap<>();
        calculateOffsets();
    }

    private void calculateOffsets(){
        var fields = schema.fields();
        Arrays.sort(fields);
        int offset = 0;
        for(var f:fields){
            offsets.put(f, offset);
            Schema.Type type = schema.type(f);
            if(type == Schema.Type.INTEGER){
                offset += Integer.BYTES;
                sizes.put(f,Integer.BYTES);
            }else if(type == Schema.Type.STRING){
                offset += RecordID.SIZE;
                sizes.put(f,RecordID.SIZE);
            }
        }
    }



}

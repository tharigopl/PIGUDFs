package com.rozar.pig.udf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/*
 * This PIG UDF will convert the data in cassandra database with the datatype as any UUIDs 
 * to UUIDs in PIG instead of displaying in the byte array datatype of PIG 
 */
public class TOUUID extends EvalFunc<String> {
	
    public String exec(Tuple input) throws IOException {
    	DataByteArray str = (DataByteArray)input.get(0);
    	byte[] bytes = str.get();
    	
    	long msb = 0;
        long lsb = 0;
        for (int in=0; in<8; in++)
            msb = (msb << 8) | (bytes[in] & 0xff);
        for (int in=8; in<16; in++)
            lsb = (lsb << 8) | (bytes[in] & 0xff);
		  	        
		return new UUID(msb,lsb).toString();        
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass()
                .getName().toLowerCase(), input), DataType.BYTEARRAY));
    }

    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
    	List<FuncSpec> funcList = new ArrayList<FuncSpec>();
        Schema s = new Schema();
        s.add(new Schema.FieldSchema(null, DataType.BYTEARRAY));
        funcList.add(new FuncSpec(this.getClass().getName(), s));
        return funcList;
    } 
}

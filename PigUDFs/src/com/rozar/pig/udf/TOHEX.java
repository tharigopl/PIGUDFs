package com.rozar.pig.udf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/*
 * This PIG UDF will convert the data in cassandra database with the datatype as any blob 
 * to Hex strings in PIG instead of displaying in the byte array datatype of PIG 
 */
public class TOHEX extends EvalFunc<String> {

	final protected static char[] hexArray = {'0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F'};
	
    public String exec(Tuple input) throws IOException {
    	DataByteArray str = (DataByteArray)input.get(0);
    	byte[] bytes = str.get();
    	char[] hexChars = new char[bytes.length * 2];
        int v;
        for ( int j = 0; j < bytes.length; j++ ) {
            v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);    	       
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

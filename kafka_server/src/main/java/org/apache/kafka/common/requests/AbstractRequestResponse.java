package org.apache.kafka.common.requests;

import java.nio.ByteBuffer;

import org.apache.kafka.common.protocol.types.Struct;

public abstract class AbstractRequestResponse {

	protected final Struct struct;
	
	public AbstractRequestResponse(Struct struct){
		this.struct = struct;
	}
	
	public Struct toStruct(){
		return struct;
	}
	
	public int sizeOf() {
        return struct.sizeOf();
    }

    /**
     * Write this object to a buffer
     */
    public void writeTo(ByteBuffer buffer) {
        struct.writeTo(buffer);
    }

    @Override
    public String toString() {
        return struct.toString();
    }

    @Override
    public int hashCode() {
        return struct.hashCode();
    }
    
    @Override
    public boolean equals(Object obj) {
    	if(obj == null){
    		return false;
    	}
    	if(obj == this){
    		return true;
    	}
    	if(!(obj instanceof AbstractRequestResponse)){
    		return false;
    	}
    	AbstractRequestResponse other = (AbstractRequestResponse) obj;
    	return struct.equals(other.struct);
    }
    
}

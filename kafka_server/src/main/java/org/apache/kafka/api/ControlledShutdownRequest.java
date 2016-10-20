package org.apache.kafka.api;

import java.nio.ByteBuffer;

import org.apache.kafka.network.streams.processor.RequestChannel;
import org.apache.kafka.network.streams.processor.RequestChannel.Request;

public class ControlledShutdownRequest extends RequestOrResponse{
	
	private static final short CurrentVersion = (short)1;
	private static final String DefaultClientId = "";

	private short versionId;
	private int correlationId;
	private String clientId;
	private int brokerId;
	
	public ControlledShutdownRequest(short versionId, int correlationId, String clientId, int brokerId) {
		if(versionId > 0 && clientId.trim().isEmpty()){
			throw new IllegalArgumentException("'clientId' must be defined of if 'versionId' > 0");
		}
		
		this.versionId = versionId;
		this.correlationId = correlationId;
		this.clientId = clientId;
		this.brokerId = brokerId;
	}
	
	public static ControlledShutdownRequest readFrom(ByteBuffer buffer){
		short versionId = buffer.getShort();
		int correlationId = buffer.getInt();
		String clientId = versionId > 0 ? String.valueOf(buffer.getShort()) : null;
		int brokerId = buffer.getInt();
		
		return new ControlledShutdownRequest(versionId, correlationId, clientId, brokerId);
	}
	
	public void writeTo(ByteBuffer buffer){
		buffer.putShort(versionId);
		buffer.putInt(correlationId);
		buffer.putShort(Short.valueOf(clientId));
		buffer.putInt(brokerId);
	}

	@Override
	protected void handleError(Throwable e, RequestChannel requestChannel, Request request) {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected String describe(boolean details) {
		// TODO Auto-generated method stub
		return null;
	}
}

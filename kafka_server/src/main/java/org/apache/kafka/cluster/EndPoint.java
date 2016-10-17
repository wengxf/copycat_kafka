package org.apache.kafka.cluster;

import java.nio.ByteBuffer;
import java.util.regex.Pattern;

import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.utils.Utils;

public class EndPoint {
	
	public String host;
	public int port;
	public SecurityProtocol protocolType;
	
	private Pattern pattern = Pattern.compile("");
	
	private String connectionString(){
		String hostport = (host == null ? ":" + port : Utils.formatAddress(host, port));
		return protocolType + "://" + hostport;
	}
	
	private void writeTo(ByteBuffer buffer){
		buffer.putInt(port);
//		writes
	}
	
//	public int sizeInBytes(){
//		
//	}
	
	public EndPoint(String host, int port, SecurityProtocol protocol) {
		this.host = host;
		this.port = port;
		this.protocolType = protocol;
	}

//	public static EndPoint readFrom(ByteBuffer byteBuffer){
//		int port = byteBuffer.getInt();
//		String host = "";
//		short protocol = byteBuffer.getShort();
//		
//		return null;
//	}
//	
//	public static EndPoint createEndPoint(){
//		return null;
//	}
//	
	
}

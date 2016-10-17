package org.apache.kafka.network.streams.processor;

import java.beans.FeatureDescriptor;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.kafka.api.RequestOrResponse;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

/**
 * 
 * @author baodekang
 *
 */
public class RequestChannel {
	
	public void getShutdownReceive(){
		
	}
	
	
	private class Session{
		private KafkaPrincipal principal;
		private InetAddress clientAddress;
		
		public Session(KafkaPrincipal principal, InetAddress clientAddress) {
			this.principal = principal;
			this.clientAddress = clientAddress;
		}
	}
	
	public class Request{
		private int processor;
		private String connectionId;
		private Session session;
		private ByteBuffer buffer;
		private Long startTimeMs;
		private SecurityProtocol securityProtocol;
		
		public Short requestId;
		
		private Map<Short, RequestOrResponse> keyToNameAndDeserializerMap;
		public Request(int processor, String connectionId, Session session, ByteBuffer buffer, Long startTimeMs,
				SecurityProtocol securityProtocol) {
			this.processor = processor;
			this.connectionId = connectionId;
			this.session = session;
			this.buffer = buffer;
			this.startTimeMs = startTimeMs;
			this.securityProtocol = securityProtocol;
			
			requestId = buffer.getShort();
		}
		
		private volatile Long requestDequeueTimeMs = -1L;
		private volatile Long apiLocalCompleteTimeMs = -1L;
		private volatile Long responseCompleteTimeMs = -1L;
		private volatile Long responseDequeueTimeMs = -1L;
		private volatile Long apiRemoteCompleteTimeMs = -1L;
	}
}

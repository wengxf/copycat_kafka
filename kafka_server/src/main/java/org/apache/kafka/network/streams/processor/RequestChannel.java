package org.apache.kafka.network.streams.processor;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.api.ControlledShutdownRequest;
import org.apache.kafka.api.FetchRequest;
import org.apache.kafka.api.RequestOrResponse;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Protocol;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestSend;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.network.KafkaMetricsGroup;

/**
 * 
 * @author baodekang
 *
 */
public class RequestChannel extends KafkaMetricsGroup{
	

	private Request AllDone;
	
	private int numProcessors;
	private int queueSize;
	private List<Integer> responseListeners;
	private ArrayBlockingQueue<RequestChannel.Request> requestQueue;
	private BlockingQueue<RequestChannel.Response>[] responseQueues;
	
	public RequestChannel(int numProcessors, int queueSize) {
		this.numProcessors = numProcessors;
		this.queueSize = queueSize;
		responseListeners = new ArrayList<>();
		requestQueue = new ArrayBlockingQueue<>(queueSize);
		responseQueues = new BlockingQueue[numProcessors];
		for(int i=0; i< numProcessors; i++){
			responseQueues[i] = new LinkedBlockingQueue();
		}
		
		ByteBuffer buffer = getShutdownReceive();
		
//		try {
//			AllDone = new Request(1, "2", new Session(KafkaPrincipal.ANONYMOUS, InetAddress.getLocalHost()), 
//					buffer, 0L, SecurityProtocol.PLAINTEXT);
//		} catch (UnknownHostException e) {
//			e.printStackTrace();
//		}
	}
	
	public void sendRequest(RequestChannel.Request request){
		try {
			requestQueue.put(request);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public void sendResponse(RequestChannel.Response response){
//		responsequ
	}
	
	public void noOperation(int processor, RequestChannel.Request request){
		
	}
	
	public void closeConnection(int processor, RequestChannel.Request request){
		
	}
	
	public RequestChannel.Request receiveRequest(long timeout) throws InterruptedException{
		return requestQueue.poll(timeout, TimeUnit.MILLISECONDS);
	}
	
	public RequestChannel.Request receiveRequest() throws InterruptedException{
		return requestQueue.take();
	}
	
	public RequestChannel.Response receiveResponse(int processor){
		RequestChannel.Response response = responseQueues[processor].poll();
		if(response != null){
			response.request.responseDequeueTimeMs = System.currentTimeMillis();
		}
		
		return response;
	}
	
	public void addResponseListener(int onResponse){
//		responseListeners = onResponse;
	}
	
	public void shutdown(){
		requestQueue.clear();
	}
	
	public ByteBuffer getShutdownReceive(){
		RequestHeader emptyRequestHeader = new RequestHeader(ApiKeys.PRODUCE.id, "", 0);
		ProduceRequest emptyProduceRequest = new ProduceRequest((short)0, 0, new HashMap<TopicPartition, ByteBuffer>());
		return RequestSend.serialize(emptyRequestHeader, emptyProduceRequest.toStruct());
	}
	
	public class RequestMetrics extends KafkaMetricsGroup{
		private Map<String, RequestMetrics> metricsMap;
		private String consumerFetchMetricsName;
		private String followFetchMetricsName;
		
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
		public int processor;
		public String connectionId;
		public Session session;
		public ByteBuffer buffer;
		public Long startTimeMs;
		public SecurityProtocol securityProtocol;
		
		public Short requestId;
		public RequestOrResponse requestObj;
		public RequestHeader header;
		public AbstractRequest body;
		
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
			keyToNameAndDeserializerMap = keyToNameAndDeserializerMap();
			requestObj = keyToNameAndDeserializerMap.get(requestId);
			if(requestObj == null){
				buffer.rewind();
				header = RequestHeader.parse(buffer);
			}
			if(requestObj == null){
				body = (header.apiKey() == ApiKeys.API_VERSIONS.id && 
						!Protocol.apiVersionSupported(header.apiKey(), header.apiVersion())) ?
								new ApiVersionsRequest() : 
								AbstractRequest.getRequest(header.apiKey(), header.apiVersion(), buffer);
			}
			
			buffer = null;
			
		}
		
		private volatile Long requestDequeueTimeMs = -1L;
		private volatile Long apiLocalCompleteTimeMs = -1L;
		private volatile Long responseCompleteTimeMs = -1L;
		private volatile Long responseDequeueTimeMs = -1L;
		private volatile Long apiRemoteCompleteTimeMs = -1L;
		
		private Map<Short, RequestOrResponse> keyToNameAndDeserializerMap(){
			Map<Short, RequestOrResponse> map = new HashMap<>();
			map.put(ApiKeys.FETCH.id, FetchRequest.readFrom(buffer));
			map.put(ApiKeys.CONTROLLED_SHUTDOWN_KEY.id, ControlledShutdownRequest.readFrom(buffer));
			return map;
		}
	}
	
	public static class Response{
		public int processor;
		public Request request;
		public Send responseSend;
		public ResponseAction responseAction;
		
		public Response(int processor, Request request, Send responseSend, ResponseAction responseAction) {
			this.processor = processor;
			this.request = request;
			this.responseSend = responseSend;
			this.responseAction = responseAction;
		}
		
		public Response(int processor, Request request, Send responseSend) {
			this(processor, request, responseSend, ResponseAction.NoOpAction);
		}
		
		public Response(Request request, Send send) {
			this(request.processor, request, send);
		}
	}
	
	public enum ResponseAction{
		SendAction(),
		NoOpAction(),
		CloseConnectionAction();
		
		private ResponseAction(){
			
		}
	}
}

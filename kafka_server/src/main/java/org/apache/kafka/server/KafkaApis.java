package org.apache.kafka.server;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.network.streams.processor.RequestChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 处理所有request的proxy类，根据requestKey决定调用具体的handler
 * @author baodekang
 *
 */
public class KafkaApis {
	
	private Logger logger = LoggerFactory.getLogger(KafkaApis.class);
	
	private RequestChannel requestChannel;
	private ReplicaManager replicaManager;
	private int brokeId;
	
	private String logIdent;
	
	public KafkaApis(RequestChannel requestChannel, ReplicaManager replicaManager, int brokeId) {
		this.requestChannel = requestChannel;
		this.replicaManager = replicaManager;
		this.brokeId = brokeId;
		this.logIdent = String.format("[KafkaApi]%d", brokeId);
	}
	
	/**
	 * 处理所有的请求dispatch到对应api
	 */
	public void handle(RequestChannel.Request request){
		if(request.requestId == ApiKeys.PRODUCE.id){
			handleProducerRequest(request);
		}
	}
	
	private void handleProducerRequest(RequestChannel.Request request){
		System.out.println("处理producer");
	}
}

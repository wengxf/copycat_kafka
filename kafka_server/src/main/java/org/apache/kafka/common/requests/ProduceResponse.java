package org.apache.kafka.common.requests;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.CollectionUtils;

/**
 * 生产返回对象
 * @author baodekang
 *
 */
public class ProduceResponse extends AbstractRequestResponse{
	
	private static final Schema CURRENT_SCHEMA = null;
	private static final String RESPONSE_KEY_VALUE = "response";
	
	//topic level field names
	private static final String TOPIC_KEY_VALUE = "topic";
	private static final String PARTITION_RESPONSE_KEY_VALUE = "partition_responses";
	private static final String THROTILE_TIME_KEY_NAME = "throttle_time_ms";
	
	//partition level field names
	private static final String PARTITION_KEY_NAME = "partition";
	private static final String ERROR_CODE_KEY_NAME = "error_code";
	
	public static final long INVALID_OFFSET = -1L;
	public static final int DEFAULT_THROTILE_TIME = 0;
	
	private static final String BASE_OFFSET_KEY_NAME = "base_offset";
	private static final String TIMESTAMP_KEY_NAME = "timestamp";
	
	private final Map<TopicPartition, PartitionResponse> responses;
	private final int throttleTime;
	
	public ProduceResponse(Map<TopicPartition, PartitionResponse> responses) {
		super(new Struct(ProtoUtils.responseSchema(ApiKeys.PRODUCE.id, 0)));
		
		initCommonFields(responses);
		this.responses = responses;
		this.throttleTime = DEFAULT_THROTILE_TIME;
	}
	
	public ProduceResponse(Map<TopicPartition, PartitionResponse> responses, int throttleTime){
		this(responses, throttleTime, ProtoUtils.latestVersion(ApiKeys.PRODUCE.id));
	}
	
	public ProduceResponse(Map<TopicPartition, PartitionResponse> responses, int throttleTime, int version){
		super(new Struct(ProtoUtils.responseSchema(ApiKeys.PRODUCE.id, 0)));
		if(struct.hasField(THROTILE_TIME_KEY_NAME)){
			struct.set(THROTILE_TIME_KEY_NAME, throttleTime);
		}
		initCommonFields(responses);
		this.responses = responses;
		this.throttleTime = throttleTime;
	}
	
	private void initCommonFields(Map<TopicPartition, PartitionResponse> responses){
		Map<String, Map<Integer, PartitionResponse>> responseByTopic = CollectionUtils.groupDataByTopic(responses);
		List<Struct> topicDatas = new ArrayList<>();
		for(Entry<String, Map<Integer, PartitionResponse>> entry: responseByTopic.entrySet()){
			Struct topicData = struct.instance(RESPONSE_KEY_VALUE);
			topicData.set(TOPIC_KEY_VALUE, entry.getKey());
			List<Struct> partitionArray = new ArrayList<>();
			for(Entry<Integer, PartitionResponse> partitionEntry : entry.getValue().entrySet()){
				
			}
		}
	}
	
	public static final class PartitionResponse{
		public short errorCode;
		public long baseOffset;
		public long timestamp;
		
		public PartitionResponse(short errorCode, long baseOffset, long timestamp) {
			this.errorCode = errorCode;
			this.baseOffset = baseOffset;
			this.timestamp = timestamp;
		}
		
		@Override
        public String toString() {
            StringBuilder b = new StringBuilder();
            b.append('{');
            b.append("error: ");
            b.append(errorCode);
            b.append(",offset: ");
            b.append(baseOffset);
            b.append(",timestamp: ");
            b.append(timestamp);
            b.append('}');
            return b.toString();
        }
	}
}

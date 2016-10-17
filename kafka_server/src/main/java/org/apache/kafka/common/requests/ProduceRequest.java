package org.apache.kafka.common.requests;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.CollectionUtils;

/**
 * 生产者请求
 * @author baodekang
 *
 */
public class ProduceRequest extends AbstractRequest{

	private static final Schema CURRENT_SHCEMA = null;
	private static final String ACKS_KEY_NAME = "acks";
	private static final String TIMEOUT_KEY_NAME = "timeout";
	private static final String TOPIC_DATA_KEY_NAME = "topic_data";
	
	//topic level field names
	private static final String TOPIC_KEY_NAME = "topic";
	private static final String PARTITION_DATA_KEY_NAME = "data";
	
	//partition level field names
	private static final String PARTITION_KEY_NAME = "partition";
	private static final String RECORD_SET_KEY_NAME = "record_set";
	
	private final short acks;
	private final int timeout;
	private final Map<TopicPartition, ByteBuffer> partitionRecords;
	
	public ProduceRequest(Struct struct) {
		super(struct);
	}
	
	public ProduceRequest(short acks, int timeout, Map<TopicPartition, ByteBuffer> partitionRecords) {
		super(new Struct(CURRENT_SHCEMA));
		
		Map<String, Map<Integer, ByteBuffer>> recordsByTopic = CollectionUtils.groupDataByTopic(partitionRecords);
		struct.set(ACKS_KEY_NAME, acks);
		struct.set(TIMEOUT_KEY_NAME, timeout);
		List<Struct> topicDatas = new ArrayList<>(recordsByTopic.size());
		for(Entry<String, Map<Integer, ByteBuffer>> entry : recordsByTopic.entrySet()){
			Struct topicData = struct.instance(TOPIC_DATA_KEY_NAME);
			topicData.set(TOPIC_KEY_NAME, entry.getKey());
			List<Struct> partitionArray = new ArrayList<>();
			for(Entry<Integer, ByteBuffer> partitionEntry : entry.getValue().entrySet()){
				ByteBuffer buffer = partitionEntry.getValue().duplicate();
				Struct part = topicData.instance(PARTITION_DATA_KEY_NAME)
										.set(PARTITION_KEY_NAME, partitionEntry.getKey())
										.set(RECORD_SET_KEY_NAME, buffer);
				partitionArray.add(part);
			}
			topicData.set(PARTITION_DATA_KEY_NAME, partitionArray.toArray());
			topicDatas.add(topicData);
		}
		
		this.acks = acks;
		this.timeout = timeout;
	}
	
	public static ProduceRequest parse(ByteBuffer buffer, int versionId){
		return null;
	}

	@Override
	public AbstractRequestResponse getErrorResponse(int versionId, Throwable e) {
		if(acks == 0){
			return null;
		}
		
//		Map<TopicPartition, V
		return null;
	}
}

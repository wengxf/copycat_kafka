package org.apache.kafka.common.requests;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.record.Record;
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
		partitionRecords = new HashMap<TopicPartition, ByteBuffer>();
		for(Object topicDataObj : struct.getArray(TOPIC_DATA_KEY_NAME)){
			Struct topicData = (Struct)topicDataObj;
			String topic = topicData.getString(TOPIC_KEY_NAME);
			for(Object partitionResponseObj : topicData.getArray(PARTITION_DATA_KEY_NAME)){
				Struct partitionResponse = (Struct)partitionResponseObj;
				int partition = partitionResponse.getInt(PARTITION_KEY_NAME);
				ByteBuffer records = partitionResponse.getBytes(RECORD_SET_KEY_NAME);
				partitionRecords.put(new TopicPartition(topic, partition), records);
			}
		}
		acks = struct.getShort(ACKS_KEY_NAME);
		timeout = struct.getInt(TIMEOUT_KEY_NAME);
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
		struct.set(TOPIC_DATA_KEY_NAME, topicDatas.toArray());
		this.acks = acks;
		this.timeout = timeout;
		this.partitionRecords = partitionRecords;
	}
	
	@Override
	public AbstractRequestResponse getErrorResponse(int versionId, Throwable e) {
		if(acks == 0){
			return null;
		}
		
		Map<TopicPartition, ProduceResponse.PartitionResponse> responseMap = new HashMap<>();
		for(Entry<TopicPartition, ByteBuffer> entry : partitionRecords.entrySet()){
			responseMap.put(entry.getKey(), new ProduceResponse.PartitionResponse(Errors.forException(e).code(),
					ProduceResponse.INVALID_OFFSET, Record.NO_TIMESTAMP));
		}
		
		switch (versionId) {
			case 0:
//				return new ProduceRequest(responseMap);
				break;
			case 1:
				break;
			case 2:
				return new ProduceResponse(responseMap, ProduceResponse.DEFAULT_THROTILE_TIME, versionId);
//				break;
		default:
			throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                    versionId, this.getClass().getSimpleName(), ProtoUtils.latestVersion(ApiKeys.PRODUCE.id)));
		}
		return null;
	}
	
	public short getAcks() {
		return acks;
	}
	
	public int getTimeout() {
		return timeout;
	}
	
	public Map<TopicPartition, ByteBuffer> getPartitionRecords() {
		return partitionRecords;
	}
	
	public static ProduceRequest parse(ByteBuffer buffer, int versionId){
		return new ProduceRequest(ProtoUtils.parseRequest(ApiKeys.PRODUCE.id, versionId, buffer));
	}
	
	public static ProduceRequest parse(ByteBuffer buffer){
		return new ProduceRequest(CURRENT_SHCEMA.read(buffer));
	}
}

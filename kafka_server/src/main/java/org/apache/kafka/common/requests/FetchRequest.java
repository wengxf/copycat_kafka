package org.apache.kafka.common.requests;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;

/**
 * 抓取数据请求
 * @author baodekang
 *
 */
public class FetchRequest extends AbstractRequest{
	
	public static final int CONSUMER_REPLICA_ID = -1;
	private static final Schema CURRENT_SHCEMA = ProtoUtils.currentRequestSchema(ApiKeys.FETCH.id);
	private static final String REPLICA_ID_KEY_NAME = "replica_id";
	private static final String MAX_WAIT_KEY_NAME = "max_wait_time";
	private static final String MIN_BYTES_KEY_NAME = "min_bytes";
	private static final String TOPICS_KEY_NAME = "topics";
	
	//topic level field names
	private static final String TOPIC_KEY_NAME = "topic";
	private static final String PARTITIONS_KEY_NAME = "partitions";
	
	//partition level field names
	private static final String PARTITION_KEY_NAME = "partition";

	public FetchRequest(Struct struct) {
		super(struct);
	}

	@Override
	public AbstractRequestResponse getErrorResponse(int versionId, Throwable e) {
		return null;
	}

}

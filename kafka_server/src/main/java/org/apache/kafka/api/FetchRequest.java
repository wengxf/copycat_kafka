package org.apache.kafka.api;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.network.streams.processor.RequestChannel;
import org.apache.kafka.network.streams.processor.RequestChannel.Request;

public class FetchRequest extends RequestOrResponse{
	
	private static short CurrentVersion = (short)2;
	private static int DefaultMaxWait = 0;
	private static int DefaultMinBytes = 0;
	private static int DefaultCorrelationId = 0;
	
	private short versionId;
	private int correlationId;
	private String clientId;
	private int replicaId;
	private int maxWait;
	private int minBytes;
	private Map<TopicPartition, PartitionFetchInfo> requestInfo;
	
	private RequestHeader header;
	
	public FetchRequest() {
		this.versionId = FetchRequest.CurrentVersion;
		this.correlationId = FetchRequest.DefaultCorrelationId;
//		this.clientId = ConsumerConfig.defaul;
//		this.replicaId = replicaId;
//		this.maxWait = maxWait;
//		this.minBytes = minBytes;
//		this.requestInfo = requestInfo;
	}
	
	public FetchRequest(Short versionId, int correlationId, String clientId, int replicaId, int maxWait, int minBytes) {
		this.versionId = versionId;
		this.correlationId = correlationId;
		this.clientId = clientId;
		this.replicaId = replicaId;
		this.maxWait = maxWait;
		this.minBytes = minBytes;
//		this.requestInfo = requestInfo;
	}

	public static FetchRequest readFrom(ByteBuffer buffer){
		short versionId = buffer.getShort();
		int correlationId = buffer.getInt();
		String clientId = String.valueOf(buffer.getShort());
		int replicaId = buffer.getInt();
		int maxWait = 0; // buffer.getInt();
		int minBytes = 0; // buffer.getInt();
		int topicCount= 0; // buffer.getInt();
		return new FetchRequest(versionId, correlationId, clientId, replicaId, maxWait, minBytes);
	}
//	
	
	public void writeTo(ByteBuffer buffer){
		buffer.putShort(versionId);
	    buffer.putInt(correlationId);
//	    writeShortString(buffer, clientId);
	    buffer.putShort(Short.valueOf(clientId));
	    buffer.putInt(replicaId);
	    buffer.putInt(maxWait);
	    buffer.putInt(minBytes);
//	    buffer.putInt(requestInfoGroupedByTopic); // topic count
//	    requestInfoGroupedByTopic.foreach {
//	      case (topic, partitionFetchInfos) =>
//	        writeShortString(buffer, topic)
//	        buffer.putInt(partitionFetchInfos.size) // partition count
//	        partitionFetchInfos.foreach {
//	          case (TopicAndPartition(_, partition), PartitionFetchInfo(offset, fetchSize)) =>
//	            buffer.putInt(partition)
//	            buffer.putLong(offset)
//	            buffer.putInt(fetchSize)
//	        }
//	    }
	}
	
	private class PartitionFetchInfo{
		private long offset;
		private int fetchSize;
		public long getOffset() {
			return offset;
		}
		public void setOffset(long offset) {
			this.offset = offset;
		}
		public int getFetchSize() {
			return fetchSize;
		}
		public void setFetchSize(int fetchSize) {
			this.fetchSize = fetchSize;
		}
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

package org.apache.kafka.common.requests;

import java.nio.ByteBuffer;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;

/**
 * 外部请求抽象类
 * @author baodekang
 *
 */
public abstract class AbstractRequest extends AbstractRequestResponse{
	
	public AbstractRequest(Struct struct) {
		super(struct);
	}
	
	public abstract AbstractRequestResponse getErrorResponse(int versionId, Throwable e);
	
//	public static AbstractRequest getRequest(int requestId, int versionId, ByteBuffer buffer){
//		ApiKeys apiKeys = ApiKeys.forId(requestId);
//		switch (apiKeys) {
//		case PRODUCE:
//			
//			break;
//		default:
//			throw new AssertionError(String.format("ApiKeys %s is not currently handled in 'getRequest', the "
//					+ "code should be updated to do so.", apiKeys));
//		}
//	}
}

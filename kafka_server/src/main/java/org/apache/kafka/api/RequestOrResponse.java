package org.apache.kafka.api;

import org.apache.kafka.network.streams.processor.RequestChannel;

public abstract class RequestOrResponse {

	protected Short requestId;
	
	protected int sizeInBytes;
	
	protected abstract void handleError(Throwable e, RequestChannel requestChannel, RequestChannel.Request request);
	
	protected abstract String describe(boolean details);
}

package org.apache.kafka.clients.producer;

public interface Callback {

	public void onCompletion(RecordMetadata metadata, Exception exception);
}

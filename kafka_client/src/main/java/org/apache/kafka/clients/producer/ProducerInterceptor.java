package org.apache.kafka.clients.producer;

import org.apache.kafka.common.Configurable;

public interface ProducerInterceptor<K, V> extends Configurable {

	public ProducerRecord<K, V> onSend(ProducerRecord<K, V> record);
	
	public void onAcknowledgement(RecordMetadata metadata, Exception exception);
	
	public void close();
}

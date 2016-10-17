package org.apache.kafka.common.serialization;

import java.io.Closeable;
import java.util.Map;

public interface Serializer<T> extends Closeable{

	public void configure(Map<String, ?> configs, boolean isKey);
	
	public byte[] serialize(String topic, T data);
	
	@Override
	void close();
}

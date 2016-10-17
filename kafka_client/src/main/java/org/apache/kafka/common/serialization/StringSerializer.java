package org.apache.kafka.common.serialization;

import java.util.Map;

public class StringSerializer implements Serializer<String>{

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		
	}

	@Override
	public byte[] serialize(String topic, String data) {
		return null;
	}

	@Override
	public void close() {
		
	}

}

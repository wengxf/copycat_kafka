package org.apache.kafka.network.streams.processor;

public interface Processor<K, V> {

	public void init(ProcessContext context);
	
	public void process(K key, V value);
	
	public void punctuate(long timestamp);
	
	public void close();
	
}

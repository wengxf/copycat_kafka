package org.apache.kafka.clients.producer;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.PartitionInfo;

/**
 * 生产端接口
 * @author baodekang
 *
 * @param <K>
 * @param <V>
 */
public interface Producer<K, V> extends Closeable{
	
	public Future<RecordMetadata> send(Producer<K, V> record);
	
	public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback);
	
	public void flush();
	
	public List<PartitionInfo> partitionsFor(String topic);
	
	public Map metrics();
	
	public void close();
	
	public void close(long timeout, TimeUnit unit);
}

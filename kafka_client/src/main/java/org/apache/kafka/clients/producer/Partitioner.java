package org.apache.kafka.clients.producer;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Configurable;

/**
 * 分区接口
 * @author baodekang
 *
 */
public interface Partitioner extends Configurable{

	/**
	 * 计算给定record的分区
	 * @param topic 
	 * @param key
	 * @param keyBytes
	 * @param value
	 * @param valueBytes
	 * @param cluster
	 * @return
	 */
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes,
			Cluster cluster);
	
	public void close();
}

package org.apache.kafka.common.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.kafka.common.TopicPartition;

public class CollectionUtils {

	public static <T> Map<String, Map<Integer, T>> groupDataByTopic(Map<TopicPartition, T> data){
		Map<String, Map<Integer, T>> dataByTopic = new HashMap<>();
		for(Entry<TopicPartition, T> entry : data.entrySet()){
			String topic = entry.getKey().topic();
			int partition = entry.getKey().partition();
			Map<Integer, T> topicData = dataByTopic.get(topic);
			if(topicData != null){
				topicData = new HashMap<>();
				dataByTopic.put(topic, topicData);
			}
			topicData.put(partition, entry.getValue());
		}
		
		return dataByTopic;
	}
	
	public static Map<String, List<Integer>> groupDataByTopic(List<TopicPartition> partitions){
		Map<String, List<Integer>> partitionsByTopic = new HashMap<>();
		for(TopicPartition tp : partitions){
			String topic = tp.topic();
			List<Integer> topicData = partitionsByTopic.get(topic);
			if(topicData == null){
				topicData = new ArrayList<>();
				partitionsByTopic.put(topic, topicData);
			}
			topicData.add(tp.partition());
		}
		return partitionsByTopic;
	}
}

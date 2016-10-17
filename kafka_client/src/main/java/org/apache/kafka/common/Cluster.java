package org.apache.kafka.common;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.utils.Utils;

/**
 * topics, partitions, 子集的集合
 * @author baodekang
 *
 */
public final class Cluster {

	private final boolean isBootstrapConfigured;
	private final List<Node> nodes;
	private final Set<String> unauthorizedTopics;
	private final Map<TopicPartition, PartitionInfo> partitionsByTopicPartition;
	private final Map<String, List<PartitionInfo>> partitionsByTopic;
	private final Map<String, List<PartitionInfo>> availablePartitionsByTopic;
	private final Map<Integer, List<PartitionInfo>> partitionsByNode;
	private final Map<Integer, Node> nodesById;
	
	public Cluster(Collection<Node> nodes, Collection<PartitionInfo> partitions, Set<String> unauthorizedTopics) {
		this(false, nodes, partitions, unauthorizedTopics);
	}
	
	private Cluster(boolean isBootstrapConfigured, Collection<Node> nodes, Collection<PartitionInfo> partitions,
			Set<String> unauthorizedTopics){
		this.isBootstrapConfigured = isBootstrapConfigured;
		List<Node> copy = new ArrayList<>(nodes);
		Collections.shuffle(copy);
		this.nodes = Collections.unmodifiableList(copy);
		this.nodesById = new HashMap<>();
		for(Node node : nodes){
			this.nodesById.put(node.id(), node);
		}
		this.partitionsByTopicPartition = new HashMap<>(partitions.size());
		for(PartitionInfo p : partitions){
			this.partitionsByTopicPartition.put(new TopicPartition(p.topic(), p.partition()), p);
		}
		
		HashMap<String, List<PartitionInfo>> partsForTopic = new HashMap<>();
		HashMap<Integer, List<PartitionInfo>> partsForNode = new HashMap<>();
		
		for(Node n : this.nodes){
			partsForNode.put(n.id(), new ArrayList<PartitionInfo>());
		}
		
		for(PartitionInfo p :partitions){
			if(!partsForTopic.containsKey(p.topic())){
				partsForTopic.put(p.topic(), new ArrayList<PartitionInfo>());
			}
			List<PartitionInfo> psTopic = partsForTopic.get(p.topic());
			psTopic.add(p);
			
			if(p.leader() != null){
				List<PartitionInfo> psNode = Utils.notNull(partsForNode.get(p.leader().id()));
				psNode.add(p);
			}
		}
		
		this.partitionsByTopic = new HashMap<>(partsForTopic.size());
		this.availablePartitionsByTopic = new HashMap<>(partsForTopic.size());
		for(Map.Entry<String, List<PartitionInfo>> entry : partsForTopic.entrySet()){
			String topic = entry.getKey();
			List<PartitionInfo> partitionList = entry.getValue();
			this.partitionsByTopic.put(topic, Collections.unmodifiableList(partitionList));
			List<PartitionInfo> availablePartitions = new ArrayList<>();
			for(PartitionInfo part : partitionList){
				if(part.leader() != null){
					availablePartitions.add(part);
				}
			}
			this.availablePartitionsByTopic.put(topic, Collections.unmodifiableList(availablePartitions));
		}
		this.partitionsByNode = new HashMap<>(partsForNode);
		for(Map.Entry<Integer, List<PartitionInfo>> entry : partsForNode.entrySet()){
			this.partitionsByNode.put(entry.getKey(), Collections.unmodifiableList(entry.getValue()));
		}
		this.unauthorizedTopics = Collections.unmodifiableSet(unauthorizedTopics);
	}
	
	public static Cluster empty(){
		return new Cluster(new ArrayList<Node>(0), new ArrayList<PartitionInfo>(0), Collections.<String>emptySet());
	}
	
	public static Cluster bootstrap(List<InetSocketAddress> addresses){
		List<Node> nodes = new ArrayList<>();
		int nodeId = -1;
		for(InetSocketAddress address : addresses){
			nodes.add(new Node(nodeId--, address.getHostString(), address.getPort()));
		}
		
		return new Cluster(true, nodes, new ArrayList<PartitionInfo>(0), Collections.<String>emptySet());
	}
	
	public Cluster withPartitions(Map<TopicPartition, PartitionInfo> partitions){
		Map<TopicPartition, PartitionInfo> combinedPartitions = new HashMap<>(this.partitionsByTopicPartition);
		combinedPartitions.putAll(partitions);
		return new Cluster(this.nodes, combinedPartitions.values(), new HashSet<>(this.unauthorizedTopics));
	}
	
	public List<Node> nodes(){
		return this.nodes;
	}
	
	public Node nodeById(int id){
		return this.nodesById.get(id);
	}
	
	public Node leaderFor(TopicPartition topicPartition){
		PartitionInfo info = partitionsByTopicPartition.get(topicPartition);
		if(info == null){
			return null;
		}else{
			return info.leader();
		}
	}
	
	/**
	 * 获取指定分区的metadata
	 * @param topicPartition
	 * @return
	 */
	public PartitionInfo partition(TopicPartition topicPartition){
		return partitionsByTopicPartition.get(topicPartition);
	}
	
	public List<PartitionInfo> partitionsForTopic(String topic){
		return partitionsByTopic.get(topic);
	}
}

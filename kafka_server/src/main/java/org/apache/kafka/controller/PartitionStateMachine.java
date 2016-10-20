package org.apache.kafka.controller;

/**
 * 分区状态管理器
 * @author baodekang
 *
 */
public class PartitionStateMachine {

	private KafkaController kafkaController;
	
	public PartitionStateMachine(KafkaController kafkaController) {
		this.kafkaController = kafkaController;
	}
}

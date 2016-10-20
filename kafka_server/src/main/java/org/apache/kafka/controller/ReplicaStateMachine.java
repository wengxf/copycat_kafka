package org.apache.kafka.controller;

/**
 * 副本状态管理器
 * @author baodekang
 *
 */
public class ReplicaStateMachine {

	private KafkaController kafkaController;
	
	public ReplicaStateMachine(KafkaController kafkaController) {
		this.kafkaController = kafkaController;
	}
}

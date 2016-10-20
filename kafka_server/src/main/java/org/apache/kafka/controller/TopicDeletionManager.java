package org.apache.kafka.controller;

import java.util.Set;

/**
 * topic删除管理器
 * @author baodekang
 *
 */
public class TopicDeletionManager {

	private KafkaController controller;
	private Set<String> initialTopicsToBeDeleted;
	private Set<String> initialTopicsIneligibleForDeletion;
	
	public TopicDeletionManager(KafkaController controller, Set<String> initialTopicsToBeDeleted, Set<String> initialTopicsIneligibleForDeletion) {
		this.controller = controller;
		this.initialTopicsIneligibleForDeletion = initialTopicsIneligibleForDeletion;
		this.initialTopicsToBeDeleted = initialTopicsToBeDeleted;
	}
}

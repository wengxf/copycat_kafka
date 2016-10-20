package org.apache.kafka.controller;

import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.KafkaScheduler;
import org.apache.kafka.ZkUtils;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.BrokerState;
import org.apache.kafka.server.KafkaConfig;
import org.apache.kafka.server.ZookeeperLeaderElector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * kafka控制器
 * 当kafka服务器的控制模块启动是激活，但并不认为当前的代理就是控制器。
 * 它仅仅注册了session过期监听和启动控制器选主
 * @author baodekang
 *
 */
public class KafkaController {
	
	private static final Logger logger = LoggerFactory.getLogger(KafkaController.class);
	
	private KafkaConfig config;
	private ZkUtils zkUtils;
	private BrokerState brokerState;
	private Time time;
	private Metrics metrics;
	private String threadNamePrefix;
	private String logIdent;
	private boolean isRunning;
	
	private ControllerContext controllerContext;
	private PartitionStateMachine partitionStateMachine;
	private ReplicaStateMachine replicaStateMachine;
	private ZookeeperLeaderElector controllerElector;
	private KafkaScheduler autoRebalanceScheduler;
	private TopicDeletionManager deleteTopicManager;
	
	
	public KafkaController(KafkaConfig config, ZkUtils zkUtils, BrokerState brokerState, Time time,
			Metrics metrics, String threadNamePrefix) {
		this.config = config;
		this.zkUtils = zkUtils;
		this.brokerState = brokerState;
		this.time = time;
		this.metrics = metrics;
		this.threadNamePrefix = threadNamePrefix;
		
		this.logIdent = "[Controller " + config.brokerId + "]";
		isRunning = true;
		
		this.controllerContext = new ControllerContext(zkUtils, config.zkSessionTimeoutMs);
		this.partitionStateMachine = new PartitionStateMachine(this);
		this.replicaStateMachine = new ReplicaStateMachine(this);
		this.controllerElector = new ZookeeperLeaderElector(controllerContext, ZkUtils.ControllerPath, config.brokerId);
		autoRebalanceScheduler = new KafkaScheduler(1);
	}
	
	public void startup(){
		controllerContext.controllerLock.lock();
		logger.info("Controller starting up");

		isRunning = true;
		controllerElector.startup();
		
		logger.info("Controller startup complete");
	}
	
	public class ControllerContext{
		public ZkUtils zkUtils;
		public int zkSessionTimeout;
		public ReentrantLock controllerLock;
		
		public ControllerContext(ZkUtils zkUtils, int zkSessionTimeout) {
			this.zkUtils = zkUtils;
			this.zkSessionTimeout = zkSessionTimeout;
			controllerLock = new ReentrantLock();
		}
	}
}

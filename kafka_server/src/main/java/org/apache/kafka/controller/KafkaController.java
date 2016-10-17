package org.apache.kafka.controller;

import org.apache.kafka.ZkUtils;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.BrokerState;
import org.apache.kafka.server.KafkaConfig;
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
	private ControllerContext controllerContext;
	private boolean isRunning;
	
	
	public KafkaController(KafkaConfig config, ZkUtils zkUtils) {
		this.config = config;
		this.zkUtils = zkUtils;
		this.isRunning = true;
		this.controllerContext = new ControllerContext(zkUtils, config.zkSessionTimeoutMs);
	}
	
	public void startup(){
		controllerContext.controllerLock.lock();
		logger.info("Controller starting up");
		isRunning = true;
	}
	
	private void registerSessionExpirationListener(){
		
	}
	
	public static Integer parseControllerId(String controllerInfoString){
		if(controllerInfoString == null){
			throw new IllegalArgumentException(String.format("Failed to parse the controller info json [%s]", controllerInfoString));
		}
		return Integer.parseInt(controllerInfoString);
	}
}

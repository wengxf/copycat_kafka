package org.apache.kafka.controller;

import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.ZkUtils;

/**
 * 控制器上下文
 * @author baodekang
 *
 */
public class ControllerContext {

	public ZkUtils zkUtils;
	public int zkSesstionTimeout;
	public ReentrantLock controllerLock;
	
	public ControllerContext(ZkUtils zkUtils, int zkSessionTimeout) {
		this.zkUtils = zkUtils;
		this.zkSesstionTimeout = zkSessionTimeout;
		
		controllerLock = new ReentrantLock();
	}
	
	
}

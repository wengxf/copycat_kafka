package org.apache.kafka.server;

import org.I0Itec.zkclient.IZkDataListener;
import org.apache.kafka.controller.KafkaController.ControllerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * leader选举
 * 基于zookeeper临时节点做leader选举，不处理session超时问题
 * 如果已经存在的leader节点挂了，将自动重新选举
 * @author baodekang
 *
 */
public class ZookeeperLeaderElector extends LeaderElector{
	
	private static final Logger logger = LoggerFactory.getLogger(ZookeeperLeaderElector.class);
	
	private ControllerContext controllerContext;
	private String electionPath;
	private int brokerId;
	private Object onBecomingLeader;
	private Object onResigningAsLeader;
	private int leaderId = -1;
	private int index;
	private LeaderChangeListener leaderChangeListener;
	
	public ZookeeperLeaderElector(ControllerContext controllerContext, String electionPath, int brokerId) {
		this.controllerContext = controllerContext;
		this.electionPath = electionPath;
		this.brokerId = brokerId;
		index = electionPath.lastIndexOf("/");
		if(index > 0){
			controllerContext.zkUtils.makeSurePersistentPathExists(electionPath.substring(0, index));
		}
		leaderChangeListener = new LeaderChangeListener();
	}
	

	@Override
	public void startup() {
		controllerContext.controllerLock.lock();
		controllerContext.zkUtils.zkClient.subscribeDataChanges(electionPath, leaderChangeListener);
	}

	@Override
	public Boolean elect() {
		return null;
	}

	@Override
	public void close() {
		
	}
	
	private class LeaderChangeListener implements IZkDataListener{

		@Override
		public void handleDataChange(String dataPath, Object data) throws Exception {
			System.out.println(dataPath + " data changed");
		}

		@Override
		public void handleDataDeleted(String dataPath) throws Exception {
			System.out.println("path: " + dataPath + " deleted");
		}
		
	}
	
}

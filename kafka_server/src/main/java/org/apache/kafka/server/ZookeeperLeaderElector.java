package org.apache.kafka.server;

import java.util.HashMap;
import java.util.Map;

import org.I0Itec.zkclient.IZkDataListener;
import org.apache.kafka.controller.ControllerContext;
import org.apache.kafka.controller.KafkaController;
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
	private int leaderId;
	private int index;
//	private leaderc
	
	public ZookeeperLeaderElector(ControllerContext context, String electionPath, int brokerId) {
		this.controllerContext = context;
		this.electionPath = electionPath;
		this.brokerId = brokerId;
		
		this.leaderId = -1;
		this.index = electionPath.indexOf("/");
		if(index > 0){
			controllerContext.zkUtils.makeSurePersistentPathExists(electionPath.substring(0, index));
		}
	}
	
	private int getControllerId(){
//		controllerContext.zkUtils.read
		return 0;
	}
	
	
	@Override
	public void startup() {
		
	}
	
	@Override
	public Boolean elect() {
		long timestamp = System.currentTimeMillis();
		Map<String, Object> map = new HashMap<>();
		map.put("version", 1);
		map.put("brokerid", brokerId);
		map.put("timestamp", timestamp);
//		leaderId = 
		return null;
	}
	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}
	
	private class LeaderChangeListener implements IZkDataListener {

		/**
		 * 当leader存储在zookeeper的信息发生改变被调用，
		 * 并在内存中记录新的leader
		 */
		@Override
		public void handleDataChange(String dataPath, Object data) throws Exception {
			controllerContext.controllerLock.lock();
			boolean amILeaderBeforeDataChange = amILeader;
			leaderId = KafkaController.parseControllerId(data.toString());
			logger.info(String.format("New leader is %d", leaderId));
			//The old leader needs to resign leadership if it is no longer the leader
			if(amILeaderBeforeDataChange && !amILeader){
//				onResigningAsLeader();
			}
		}

		@Override
		public void handleDataDeleted(String dataPath) throws Exception {
			
		}
		
	}
}

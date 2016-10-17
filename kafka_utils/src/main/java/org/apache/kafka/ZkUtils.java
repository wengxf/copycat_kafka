
package org.apache.kafka;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.kafka.common.config.ConfigException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;

/**
 * zookeeper工具类
 * @author baodekang
 *
 */
public class ZkUtils {

	private static final String ConsumersPath = "/consumers";
	private static final String BrokerIdsPath = "/brokers/ids";
	private static final String BrokerTopicsPath = "/brokers/topics";
	private static final String ControllerPath = "/controller";
	private static final String ControllerEpochPath = "/controller_epoch";
	private static final String ReassignPartitionsPath = "/admin/reassign_partitions";
	private static final String DeleteTopicsPath = "/admin/delete_topics";
	private static final String PreferredReplicaLeaderElectionPath = "/admin/preferred_replica_election";
	private static final String BrokerSequenceIdPath = "/brokers/seqid";
	private static final String IsrChangeNotificationPath = "/isr_change_notification";
	private static final String EntityConfigPath = "/config";
	private static final String EntityConfigChangesPath = "/config/changes";
	
	public ZkClient zkClient;
	private ZkConnection zkConnection;
	private static boolean isSecure;
	
	private static List<String> persistentZkPaths = new ArrayList<String>();
	private static List<ACL> DefaultAcls;
	
	static {
		persistentZkPaths.add(ConsumersPath);
		persistentZkPaths.add(BrokerIdsPath);
		persistentZkPaths.add(BrokerTopicsPath);
		persistentZkPaths.add(EntityConfigChangesPath);
//		persistentZkPaths.add(getEntityConfigRootPath(ConfigType.Topic));
//		persistentZkPaths.add(getEntityConfigRootPath(ConfigType.Client));
		persistentZkPaths.add(DeleteTopicsPath);
		persistentZkPaths.add(BrokerSequenceIdPath);
		persistentZkPaths.add(IsrChangeNotificationPath);
		
		DefaultAcls = new ArrayList<ACL>();
		if(isSecure){
			DefaultAcls.addAll(ZooDefs.Ids.CREATOR_ALL_ACL);
			DefaultAcls.addAll(ZooDefs.Ids.READ_ACL_UNSAFE);
		}else{
			DefaultAcls.addAll(ZooDefs.Ids.OPEN_ACL_UNSAFE);
		}
	}
	
	public ZkUtils(ZkClient zkClient, ZkConnection zkConnection, boolean isSecure){
		this.zkClient = zkClient;
		this.zkConnection = zkConnection;
		this.isSecure = isSecure;
	}
	
	public static ZkUtils getInstance(String zkUrl, int sessionTimeout, int connectionTimeout, boolean isZkSecurityEnabled){
		Object[] objs = createZkClientAndConnection(zkUrl, sessionTimeout, connectionTimeout);
		return new ZkUtils((ZkClient)objs[0], (ZkConnection)objs[1], isZkSecurityEnabled);
	}
	
	public ZkUtils(ZkClient zkClient, boolean isZkSecurityEnabled){
		this(zkClient, null, isZkSecurityEnabled);
	}
	
	public ZkClient createZkClient(String zkUrl, int sessionTimeout, int connectionTimeout){
		return new ZkClient(zkUrl, sessionTimeout, connectionTimeout, new ZKStringSerializer());
	}
	
	public static Object[] createZkClientAndConnection(String zkUrl, int sessionTimeout, int connectionTimeout){
		ZkConnection zkConnection = new ZkConnection(zkUrl, sessionTimeout);
		ZkClient zkClient = new ZkClient(zkConnection, connectionTimeout, new ZKStringSerializer());
		Object[] objs = {zkClient, zkConnection};
		return objs;
	}
	
	/**
	 * 创建通用节点
	 */
	public void setupCommonPaths(){
		for(String path : persistentZkPaths){
			makeSurePersistentPathExists(path);
		}
	}
	
	public void makeSurePersistentPathExists(String path, List<ACL> acls){
		List<ACL> acl = (path == null || path.isEmpty() || path.equals(ConsumersPath)) ? ZooDefs.Ids.OPEN_ACL_UNSAFE : 
						(acls == null || acls.size() == 0 ? DefaultAcls : acls);
		if(!zkClient.exists(path)){
			ZkPath.createPersistent(zkClient, path, true, acl);
		}
	}
	
	public void makeSurePersistentPathExists(String path){
		makeSurePersistentPathExists(path, null);
	}
	
	private static class ZKStringSerializer implements ZkSerializer{

		@Override
		public byte[] serialize(Object data) throws ZkMarshallingError {
			try {
				return ((String)data).getBytes("UTF-8");
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
			return null;
		}

		@Override
		public Object deserialize(byte[] bytes) throws ZkMarshallingError {
			if(bytes == null){
				return null;
			}
			try {
				return new String(bytes, "UTF-8");
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
			return null;
		}
	}
	
	private static class ZkPath{
		private static boolean isNamespacePresent = false;
		
		public static void checkNamespace(ZkClient client){
			if(isNamespacePresent){
				return;
			}
			
			if(!client.exists("/")){
				throw new ConfigException("Zookeeper namespace does not exsits!");
			}
			
			isNamespacePresent = true;
		}
		
		public static void resetNamespaceCheckedState(){
			isNamespacePresent = false;
		}
		
		public static void createPersistent(ZkClient client, String path, Object data, List<ACL> acls){
			checkNamespace(client);
			client.createPersistent(path, data, acls);
		}
		
		public static void createPersistent(ZkClient client, String path, boolean createParents, List<ACL> acls){
			checkNamespace(client);
			client.createPersistent(path, createParents, acls);
		}
		
		public static void createEphemeral(ZkClient client, String path, Object data, List<ACL> acls){
			checkNamespace(client);
			client.createEphemeral(path, data, acls);
		}
		
		public static void createPersistentSequential(ZkClient client, String path, Object data, List<ACL> acls){
			checkNamespace(client);
			client.createPersistentSequential(path, data, acls);
		}
		
	}
	
	public static void main(String[] args) {
		String zkUrl = "192.168.59.133:2181";
		ZkUtils zkUtils = ZkUtils.getInstance(zkUrl, 3000, 6000, false);
		zkUtils.setupCommonPaths();
	}
}

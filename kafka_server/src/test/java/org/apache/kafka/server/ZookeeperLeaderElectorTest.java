package org.apache.kafka.server;

import org.apache.kafka.ZkUtils;
import org.junit.Test;

public class ZookeeperLeaderElectorTest {

	@Test
	public void createPath(){
		ZkUtils zkUtils = ZkUtils.getInstance("192.168.59.137:2181", 6000, 6000, false);
		
		zkUtils.makeSurePersistentPathExists("/controller");
	}
}

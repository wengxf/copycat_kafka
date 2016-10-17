package org.apache.kafka.server;

/**
 * leader选举
 * 如果已经存在的leader挂了，该接口自动调用leader stat change回调
 * 自动重新选举
 * @author baodekang
 *
 */
public abstract class LeaderElector {

	public abstract void startup();
	
	public Boolean amILeader;
	
	public abstract Boolean elect();
	
	public abstract void close();
}

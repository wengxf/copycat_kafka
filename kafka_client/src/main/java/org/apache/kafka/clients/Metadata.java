package org.apache.kafka.clients;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.common.Cluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 封装元数据逻辑的类
 * 该类被client端和发送端线程共享
 * @author baodekang
 *
 */
public class Metadata {

	private static final Logger logger = LoggerFactory.getLogger(Metadata.class);
	
	private final long refreshBackoffMs;
	private final long metadataExpireMs;
    private int version;
    private long lastRefreshMs;
    private long lastSuccessfulRefreshMs;
    private Cluster cluster;
    private boolean needUpdate;
    private final Set<String> topics;
//    private final List<Listener> listeners;
    private boolean needMetadataForAllTopics;
    
    public Metadata() {
        this(100L, 60 * 60 * 1000L);
    }
    
    public Metadata(long refreshBackoffMs, long metadataExpireMs) {
        this.refreshBackoffMs = refreshBackoffMs;
        this.metadataExpireMs = metadataExpireMs;
        this.lastRefreshMs = 0L;
        this.lastSuccessfulRefreshMs = 0L;
        this.version = 0;
//        this.cluster = Cluster.empty();
        this.needUpdate = false;
        this.topics = new HashSet<String>();
//        this.listeners = new ArrayList<>();
        this.needMetadataForAllTopics = false;
    }
    
    public Cluster fetch(){
    	return cluster;
    }
    
    public synchronized void add(String topic){
    	topics.add(topic);
    }
    
    public synchronized long timeToNextUpdate(long nowMs){
    	long timeToExpire = needUpdate ? 0 : Math.max(this.lastSuccessfulRefreshMs + this.metadataExpireMs - nowMs, 0);
        long timeToAllowUpdate = this.lastRefreshMs + this.refreshBackoffMs - nowMs;
        return Math.max(timeToExpire, timeToAllowUpdate);
    }
    
    public synchronized int requestUpdate(){
    	this.needUpdate = true;
    	return this.version;
    }
    
    public synchronized boolean updateRequested(){
    	return this.needUpdate;
    }
    
    public synchronized void awaitUpdate(final int lastVersion, final long maxWaitMs) throws InterruptedException, TimeoutException {
    	if(maxWaitMs < 0){
    		throw new IllegalArgumentException("Max time to wait for metadata updates should not be < 0 milli seconds");
    	}
    	
    	long begin = System.currentTimeMillis();
    	long remainingWaitMs = maxWaitMs;
    	while(this.version <= lastVersion){
    		if(remainingWaitMs != 0){
    			wait(remainingWaitMs);
    		}
    		long elapsed = System.currentTimeMillis() -begin;
    		if(elapsed >= maxWaitMs){
    			throw new TimeoutException("Failed to update metadata after " + maxWaitMs + "ms.");
    		}
    		remainingWaitMs = maxWaitMs - elapsed;
    	}
    }
    
    public synchronized boolean containsTopic(String topic){
    	return this.topics.contains(topic);
    }
}

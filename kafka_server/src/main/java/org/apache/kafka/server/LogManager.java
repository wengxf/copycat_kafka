package org.apache.kafka.server;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.KafkaScheduler;
import org.apache.kafka.Scheduler;
import org.apache.kafka.SchedulerTask;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.log.CleanerConfig;
import org.apache.kafka.log.LogCleaner;
import org.apache.kafka.log.LogConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * kafka文件存储系统管理
 * 负责log的创建，检索及清理，所有的读写操作由单个的日志实例来代理
 * @author baodekang
 *
 */
public class LogManager {
	
	private Logger logger = LoggerFactory.getLogger(LogManager.class);
	
	private File[] logDirs;
	private Map<String, LogConfig> topicConfigs;
	private LogConfig defaultConfig;
	private CleanerConfig cleanerConfig;
	private int ioThreads;
	private Long flushCheckMs;
	private Long flushCheckpointMs;
	private Long retentionCheckMs;
	private Scheduler scheduler;
	private BrokerState brokerState;
	private Time time;
	
	private String RecoveryPointCheckpointFile = "recovery-point-offset-checkpoint";
	private String LockFile = ".lock";
	private Long InitialTaskDelayMs = 30 * 1000L;
	private Object logCreationOrDeletionLock = new Object();
	private LogCleaner cleaner;
	
	public LogManager(File[] logDirs, Map<String, LogConfig> topicConfigs, LogConfig defaultConfig,
						CleanerConfig cleanerConfig, int ioThreads, Long flushCheckMs, Long flushCheckpointMs,
						Long retentionCheckMs, KafkaScheduler scheduler, BrokerState brokerState, Time time) {
		this.logDirs = logDirs;
		this.topicConfigs = topicConfigs;
		this.defaultConfig = defaultConfig;
		this.cleanerConfig = cleanerConfig;
		this.ioThreads = ioThreads;
		this.flushCheckMs = flushCheckMs;
		this.flushCheckpointMs = flushCheckpointMs;
		this.retentionCheckMs = retentionCheckMs;
		this.scheduler = scheduler;
		this.brokerState = brokerState;
		this.time = time;
		
		this.cleaner = new LogCleaner();
	}
	
	/**
	 * 启动后台线程flush日志和日志清除
	 */
	public void startup(){
		/** Schedule the cleanup task to delete old logs**/
		logger.info(String.format("Starting log cleanup with a period of %d ms.", retentionCheckMs));
		if(scheduler != null){
			scheduler.schedule("kafka-log-retention", new SchedulerTask<LogManager>(this, "cleanupLogs"), 0L, 0L, TimeUnit.MILLISECONDS);
		}
		logger.info(String.format("Starting log flusher with a default period of %d ms.", flushCheckMs));
		scheduler.schedule("kafka-log-flusher", new SchedulerTask<LogManager>(this, "flushDirtyLogs"), 0L, 0L, TimeUnit.MICROSECONDS);
		scheduler.schedule("kafka-recovery-point-checkpoint", new SchedulerTask<LogManager>(this, "checkpointRecoveryPointOffsets"), 0L, 0L, TimeUnit.MICROSECONDS);
	}
	
	public void shutdown(){
		logger.info("Shutting down.");
		
		if(cleaner != null){
		}
		
		for(File dir : logDirs){
			logger.debug("Flushing and closing logs at " + dir);
			ExecutorService pool = Executors.newFixedThreadPool(ioThreads);
//			logs
		}
	}
	
	public void cleanupLogs(){
		logger.debug("Beginning log cleanup...");
//		int total = 0;
//
//		long startTime = System.currentTimeMillis();
//		for()
	}
	
	public void flushDirtyLogs(){
		System.out.println("flushDirtyLogs");
	}
	
	public void checkpointRecoveryPointOffsets(){
		System.out.println("checkpointRecoveryPointOffsets");
	}
}

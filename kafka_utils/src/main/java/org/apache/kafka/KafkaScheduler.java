package org.apache.kafka;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 负责副本管理和日志管理调度等等
 * @author baodekang
 *
 */
public class KafkaScheduler implements Scheduler{
	
	private Logger logger = LoggerFactory.getLogger(KafkaScheduler.class);
	private ScheduledThreadPoolExecutor executor = null;
	private AtomicInteger schedularThreadId = new AtomicInteger(0);
	
	private int threads;
	private String threadNamePrefix;
	private boolean daemon;
	
	public KafkaScheduler(int threads){
		this(threads, "kafka-scheduler-", true);
	}
	
	public KafkaScheduler(int threads, String threadNamePrefix, boolean daemon) {
		this.threads = threads;
		this.threadNamePrefix = threadNamePrefix;
		this.daemon = daemon;
	}

	@Override
	public void startup() {
		logger.debug("Initializing task scheduler.");
		synchronized (this) {
			if(isStarted){
				throw new IllegalStateException("This scheduler has already been started!");
			}
			executor = new ScheduledThreadPoolExecutor(threads);
			executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
			executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
			executor.setThreadFactory(new ThreadFactory() {
				
				@Override
				public Thread newThread(Runnable runnable) {
					return Utils.newThread(threadNamePrefix + schedularThreadId.getAndIncrement(), runnable, daemon);
				}
			});
		}
	}

	@Override
	public void shutdown() {
		logger.debug("Shutting down task scheduler.");
		ScheduledThreadPoolExecutor cachedExecutor = this.executor;
		if(cachedExecutor != null){
			synchronized(this){
				cachedExecutor.shutdown();
				this.executor = null;
			}
			try {
				cachedExecutor.awaitTermination(1, TimeUnit.DAYS);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void schedule(final String name, final SchedulerTask<?> task, Long delay, Long period, TimeUnit unit) {
		logger.debug("Scheduling task %s with delay %d ms and period %d ms.".format(name, TimeUnit.MILLISECONDS.convert(delay, unit), TimeUnit.MILLISECONDS.convert(period, unit)));
		synchronized (this) {
			Runnable runnable = new Runnable() {
				
				@Override
				public void run() {
					try {
						logger.info(String.format("Beginning exection of shceduled task '%s'.", name));
						task.invoke();
					} catch (Exception e) {
						e.printStackTrace();
					} finally{
						logger.info(String.format("Completed execution of shceduled task %s.", name));
					}
				}
			};
			
			if(period > 0){
				executor.scheduleAtFixedRate(runnable, delay, period, unit);
			}else{
				executor.schedule(runnable, delay, unit);
			}
		}
	}
	
	public static void main(String[] args) {
		System.out.println(String.format("Beginning exection of shceduled task '%s'.", "abc"));
	}

}

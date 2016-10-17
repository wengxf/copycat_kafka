package org.apache.kafka;

import java.util.concurrent.TimeUnit;

public interface Scheduler {

	public void startup();
	
	public void shutdown();
	
	public boolean isStarted = false;
	
	public void schedule(String name,SchedulerTask<?> task, Long delay, Long period, TimeUnit unit);
}

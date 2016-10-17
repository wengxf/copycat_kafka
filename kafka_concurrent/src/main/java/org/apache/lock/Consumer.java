package org.apache.lock;

import java.util.concurrent.locks.Lock;

public class Consumer implements Runnable{
	
	private Lock lock;
	
	public Consumer(Lock lock) {
		this.lock = lock;
	}

	@Override
	public void run() {
		int count =10;
		while(count > 0){
			try {
				lock.lock();
				count --;
				System.out.println("B");
			} finally{
				lock.unlock();
				try {
					Thread.sleep(91L);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				
			}
		}
	}

}

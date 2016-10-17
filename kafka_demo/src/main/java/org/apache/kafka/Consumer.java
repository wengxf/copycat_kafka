package org.apache.kafka;

import java.util.concurrent.ArrayBlockingQueue;

/**
 * 消费端
 * @author baodekang
 *
 */
public class Consumer extends Thread{

	private ArrayBlockingQueue<String> queue;
	
	public Consumer(ArrayBlockingQueue<String> queue) {
		this.queue = queue;
	}
	
	@Override
	public void run() {
		consumer();
	}
	
	public void consumer(){
//		while(true){
			try {
				String value = queue.take();
				System.out.println(value);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
//		}
	}
}

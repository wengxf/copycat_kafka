package org.apache.kafka;

import java.util.concurrent.ArrayBlockingQueue;

/**
 * 生产端
 * @author baodekang
 *
 */
public class Producer extends Thread{
	
	private ArrayBlockingQueue<String> queue;
	
	public Producer(ArrayBlockingQueue<String> queue) {
		this.queue = queue;
	}

	@Override
	public void run() {
		producer();
	}
	
	public void producer(){
//		while(true){
			try {
				queue.put("hello world");
				System.out.println("队列size：" + queue.size());
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
//		}
	}
}

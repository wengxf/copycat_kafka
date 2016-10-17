package org.apache.kafka;

import java.util.concurrent.ArrayBlockingQueue;

public class Demo {

	public static void main(String[] args) {
		ArrayBlockingQueue<String> queue = new ArrayBlockingQueue<>(10);
		Consumer consumer = new Consumer(queue);
		consumer.start();
		
//		Producer producer = new Producer(queue);
//		producer.start();
	}
}

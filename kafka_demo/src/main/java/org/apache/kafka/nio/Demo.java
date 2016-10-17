package org.apache.kafka.nio;

public class Demo {

	public static void main(String[] args) {
		new Thread(new Server()).start();
	}
}

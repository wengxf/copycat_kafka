package org.apache;

public class Test {

	public static final Object obj = new Object();
	
	public static void main(String[] args) {
		new Thread(new Producer()).start();
		new Thread(new Consumer()).start();
	}
}

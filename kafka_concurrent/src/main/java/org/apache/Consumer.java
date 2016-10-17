package org.apache;

public class Consumer implements Runnable{

	@Override
	public void run() {
		int count = 10;
		while(count > 0){
			synchronized (Test.obj) {
				System.out.println("B");
				count--;
				
				Test.obj.notify();
				
				try {
					Test.obj.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

}

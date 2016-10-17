package org.apache.kafka;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 阻塞队列
 * @author baodekang
 *
 */
public class BlockingQueue<E> {

	private Object[] items;
	private int size;
	private Condition notEmpty;
	private Condition notFull;
	private ReentrantLock lock;
	private int putIndex;
	private int takeIndex;
	
	public BlockingQueue(int capacity){
		items = new Object[capacity];
		lock = new ReentrantLock();
		notEmpty = lock.newCondition();
		notFull = lock.newCondition();
	}
	
	public void put(E item) throws InterruptedException{
		if(item == null){
			throw new IllegalArgumentException("参数不能为空!");
		}
		lock.lockInterruptibly();
		try {
			try {
				while(size == items.length){
					notFull.await();
				}
			} catch (Exception e) {
				notFull.signal();
				throw e;
			}
			insert(item);
		} finally{
			lock.unlock();
		}
	}

	private void insert(E item) {
		items[putIndex] = item;
		putIndex = inc(putIndex);
		++size;
		
		notEmpty.signal();
	}
	
	/**
	 * 循环插入
	 * @param i
	 * @return
	 */
	private int inc(int i){
		return ++i == items.length ? 0 : i;
	}
	
	public Object take() throws InterruptedException{
		lock.lockInterruptibly();
		try {
			try {
				while(size == 0){
					notEmpty.await();
				}
			} catch (Exception e) {
				notEmpty.signal();
				e.printStackTrace();
			}
			return extract();
		} finally{
			lock.unlock();
		}
	}
	
	private Object extract(){
		Object x= items[takeIndex];
		items[takeIndex] = null;
		takeIndex = inc(takeIndex);
		--size;
		notFull.signal();
		return x;
	}
	
	public int getSize() {
		return size;
	}
}

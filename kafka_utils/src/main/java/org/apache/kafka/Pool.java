package org.apache.kafka;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class Pool<K, V>{
	
	private ConcurrentMap<K, V> pool = new ConcurrentHashMap<K, V>();
	private Object createLock = new Object();
	
	public Pool(Map<K, V> m) {
		for(K key : m.keySet()){
			pool.put(key, m.get(key));
		}
	}
	
	public void put(K k, V v){
		pool.put(k, v);
	}
	
	public void putIfNotExists(K k, V v){
		pool.putIfAbsent(k, v);
	}
	
	public void getAndMaybePut(K key){
		
	}
	
	
	public boolean contains(K id){
		return pool.containsKey(id);
	}
	
	public V get(K key){
		return pool.get(key);
	}
	
	public V remove(K key){
		return pool.remove(key);
	}
	
	public boolean remove(K key, V value){
		return pool.remove(key, value);
	}
	
	public Set<K> keys(){
		return pool.keySet();
	}
	
	public Iterable<V> values(){
		return new ArrayList<V>(pool.values());
	}
	
	public void clear(){
		pool.clear();
	}
	
	public int size(){
		return pool.size();
	}
	
//	public Iterator iterator() {
//		Iterator<Entry<K, V>> iterator = pool.entrySet().iterator();
//		while(iterator.hasNext()){
//			Entry<K, V> entry = iterator.next();
//		}
//		return null;
//	}

	
}

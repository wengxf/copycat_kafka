package org.apache.kafka.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;

public class SocketServer {
	
	
	
	public void startup(){
		
	}
	
	public class Acceptor implements Runnable {
		
		private boolean isRunning = true;
		private String host;
		private int port;
		private Selector selector;
		private ServerSocketChannel serverChannel;
		private int recvBufferSize;
		
		public Acceptor(String host, int port) throws IOException {
			this.host = host;
			this.port = port;
			selector = Selector.open();
			serverChannel = openServerSocket(host, port);
		}
		
		public ServerSocketChannel openServerSocket(String host, int port){
			InetSocketAddress address = null;
			if(host == null || host.trim().isEmpty()){
				address = new InetSocketAddress(port);
			}else{
				new InetSocketAddress(host, port);
			}
			
			ServerSocketChannel ssc = null;
			try {
				ssc = ServerSocketChannel.open();
				ssc.configureBlocking(false);
				ssc.socket().setReceiveBufferSize(recvBufferSize);
				ssc.socket().bind(address);
			} catch (Exception e) {
				e.printStackTrace();
			} 
			
			return ssc;
		}

		@Override
		public void run() {
			try {
				serverChannel.register(selector, SelectionKey.OP_ACCEPT);
				while(isRunning){
					int ready = selector.select();
					if(ready > 0){
						Set<SelectionKey> keys = selector.keys();
						Iterator<SelectionKey> iterator = keys.iterator();
						while(iterator.hasNext()){
							SelectionKey key = iterator.next();
							iterator.remove();
							if(key.isAcceptable()){
//								accep();
							}
						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public class Dispatcher implements Runnable{
		private Object guard = new Object();

		public void register(Connection conn){
			synchronized(guard){
			}
		}

		@Override
		public void run() {
			
		}
	}
	
	public class DispatcherEventHandler {
		
		public void onReadableEvent(final Connection conn){
//			ByteBuffer readBuffer = allocateMemory();
//			conn.getChannel().read(readBuffer);
		}
		
		public void onWriteableEvent(){
			
		}
	}
	
	public class Connection{
		
		private SocketChannel channel;
		
		public Connection(SocketChannel channel) {
			this.channel = channel;
		}

		public SocketChannel getChannel() {
			return channel;
		}
		
	}
}

package org.apache.kafka.nio;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;

public class Server implements Runnable{

	@Override
	public void run() {
		try {
			Selector selector = Selector.open();
			ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
			ServerSocket serverSocket = serverSocketChannel.socket();
			serverSocket.setReuseAddress(true);
			serverSocket.bind(new InetSocketAddress(8080));
			serverSocketChannel.configureBlocking(false);
			serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
			
			System.out.println("服务器启动");
			while(true){
				int readyChannels = selector.select();
				if(readyChannels == 0){
					continue;
				}
				Set<SelectionKey> keys = selector.selectedKeys();
				Iterator<SelectionKey> iterator = keys.iterator();
				while(iterator.hasNext()){
					SelectionKey key = iterator.next();
					if(key.isAcceptable()){
						ServerSocketChannel server = (ServerSocketChannel) key.channel();
						SocketChannel socketChannel = server.accept();
						if(socketChannel != null){
							System.out.println("接收到请求！。。。。");
							socketChannel.configureBlocking(false);
							socketChannel.register(selector, SelectionKey.OP_READ);
						}
					}else if(key.isReadable()){
						SocketChannel socketChannel = (SocketChannel) key.channel();
						String requestStr = receive(socketChannel);
						if(requestStr.length() > 0){
							new Thread(new Processor(requestStr)).start();
						}
					}else if(key.isWritable()){
						SocketChannel socketChannel = (SocketChannel) key.channel();
						socketChannel.shutdownInput();
						socketChannel.close();
					}
					
					iterator.remove();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private String receive(SocketChannel socketChannel) throws IOException{
		ByteBuffer buffer = ByteBuffer.allocate(1024);
		byte[] bytes = null;
		int size = 0;
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		while((size = socketChannel.read(buffer)) > 0){
			buffer.flip();
			bytes = new byte[size];
			buffer.get(bytes);
			baos.write(bytes);
			buffer.clear();
		}
		bytes = baos.toByteArray();
		return new String(bytes);
	}

	
	private static class Processor implements Runnable{

		private String requestHeader;
		
		public Processor(String requestHeader) {
			this.requestHeader = requestHeader;
		}
		
		@Override
		public void run() {
			System.out.println(requestHeader);
		}
	}
}

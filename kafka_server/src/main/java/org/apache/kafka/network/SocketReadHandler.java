package org.apache.kafka.network;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SocketReadHandler implements Runnable{
	
	private static Logger logger = LoggerFactory.getLogger(SocketReadHandler.class);
	private SocketChannel socket;
	private SelectionKey key;
	
	private final int READING = 0, SENDING = 1; 
	private int STATE = READING;
	
	public SocketReadHandler(Selector selector, SocketChannel channel) throws IOException{
		this.socket = socket;
		socket.configureBlocking(false);
		key = socket.register(selector, 0);
		
		key.attach(this);
		key.interestOps(SelectionKey.OP_READ);
		selector.wakeup();
	}

	@Override
	public void run() {
		ByteBuffer input = ByteBuffer.allocate(1024);
		input.clear();
		try {
			int bytesRead = socket.read(input);
			//TODO:激活线程池，处理这些request
			System.out.println(bytesRead);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

}

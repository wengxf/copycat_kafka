package org.apache.kafka.network;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;

/**
 * nio socket服务器，线程模型是：1个Acceptor线程处理新连接，Acceptor还有多个线程器线程，
 * 每个处理器线程拥有自己的selector和多个读socket请求Handler处理。handler线程处理请求并
 * 产生响应写给处理器线程
 * @author baodekang
 *
 */
public class SocketServer implements Runnable{
	
	private Selector selector;
	
	private ServerSocketChannel servChannel;
	
	private volatile boolean stop;
	
	public SocketServer(int port){
		try {
			selector = Selector.open();
			servChannel = ServerSocketChannel.open();
			servChannel.configureBlocking(false);
			servChannel.socket().bind(new InetSocketAddress(port), 1024);
			servChannel.register(selector, SelectionKey.OP_ACCEPT);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	public void stop(){
		this.stop = true;
	}
	
	@Override
	public void run() {
		while(!stop){
			try {
				selector.select(1000);
				Set<SelectionKey> selectedKeys = selector.selectedKeys();
				Iterator<SelectionKey> it = selectedKeys.iterator();
				SelectionKey key = null;
				while(it.hasNext()){
					key = it.next();
					it.remove();
				}
				try {
					handleInput(key);
				} catch (Exception e) {
					if(key != null){
						key.cancel();
						if(key.channel() != null){
							key.channel().close();
						}
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		if(selector != null){
			try {
				selector.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private void handleInput(SelectionKey key) throws IOException{
		if(key.isValid()){
			if(key.isAcceptable()){
				ServerSocketChannel ssc = (ServerSocketChannel)key.channel();
				SocketChannel sc = ssc.accept();
				sc.configureBlocking(false);
				sc.register(selector, SelectionKey.OP_READ);
			}
			if(key.isReadable()){
				SocketChannel sc = (SocketChannel)key.channel();
				ByteBuffer readBuffer = ByteBuffer.allocate(1024);
				int readBytes = sc.read(readBuffer);
				if(readBytes > 0){
					readBuffer.flip();
					byte[] bytes = new byte[readBuffer.remaining()];
					readBuffer.get(bytes);
					String body = new String(bytes, "UTF-8");
					System.out.println("The time server receive order:" + body);
				}else if(readBytes < 0){
					key.cancel();
					sc.close();
				}else{
					;
				}
			}
		}
	}
//	private KafkaConfig config;
//	private Metrics metrics;
//	private Time time;
//	private Map<SecurityProtocol, EndPoint> endpoints;
//	private int numProcessorThreads;
//	private int maxQueuedRequests;
//	private int totalProcessorThreads;
//	
//	private int maxConnectionPerIp;
//	private int maxConnectionsPerIpOverrides;
//	private String logIdent;
////	private RequestChannel requestChannel;
//	private Processor[] processors;
//	private Map<EndPoint, Acceptor> acceptors;
//	private ConnectionQuotas connectionQuotas;
////	private 
//	
//	public SocketServer(KafkaConfig config, Metrics metrics, Time time) {
//		this.config = config;
//		this.metrics = metrics;
//		this.time = time;
//		
//		endpoints = config.listeners;
//		numProcessorThreads = config.NumNetworkThreads;
//		maxQueuedRequests = config.queuedMaxRequests;
//		totalProcessorThreads = numProcessorThreads * endpoints.size();
//		maxConnectionPerIp = config.maxConnectionsPerIp;
//		maxConnectionsPerIpOverrides = config.maxConnectionsPerIp;
//		logIdent = "[Socket Server on Broker" + config.brokerId + "]";
//		processors = new Processor[totalProcessorThreads];
//	}
//
//	public void startup() throws IOException{
//		synchronized (this) {
//			connectionQuotas = new ConnectionQuotas();
//			int sendBufferSize = config.SocketSendBufferBytes;
//			int recvBufferSize = config.SocketReceiveBufferBytes;
//			int brokerId = config.brokerId;
//			int processorBeginIndex = 0;
//			for(EndPoint endPoint : endpoints.values()){
//				SecurityProtocol protocol = endPoint.protocolType;
//				int processorEndIndex = processorBeginIndex + numProcessorThreads;
//				for(int i =processorBeginIndex; i < processorEndIndex; i++){
//					processors[i] = newProcessor(i, connectionQuotas, protocol);
//				}
////				Processor[] dest = new Processor[];
////				System.arraycopy(processors, processorBeginIndex, dest, processorEndIndex, processorEndIndex - processorBeginIndex);
//				Acceptor acceptor = new Acceptor(endPoint, sendBufferSize, recvBufferSize, brokerId, 
//						Arrays.copyOfRange(processors, processorBeginIndex, processorEndIndex), connectionQuotas);
//				acceptors.put(endPoint, acceptor);
//				Utils.newThread(String.format("kafka-socket-accptor-%s-%d", protocol.toString(), endPoint.toString())
//						, acceptor, false).start();
//				processorBeginIndex = processorEndIndex;
//			}
//		}
//	}
//	
//	public void shutdown(){
//		
//	}
//	
//	public void boundPort(){
//		
//	}
//	
//	protected Processor newProcessor(int id, ConnectionQuotas connectionQuotas, SecurityProtocol protocol){
//		return new Processor(id, 
//					config.SocketRequestMaxBytes, 
//					connectionQuotas, 10000, protocol, 
//					config.values(), metrics);
//	}
//	
//	private int connectionCount(){
//		return 0;
//	}
//	
//	private void processor(int index){
//		
//	}
//	
//	private class Acceptor extends AbstractServerThread{
//		private EndPoint endPoint;
//		private int sendBufferSize;
//		private int recvBufferSize;
//		private int brokerId;
//		private Processor[] processors;
//		
//		private ServerSocketChannel serverChannel;
//		private Selector selector;
//		
//		private Logger logger = LoggerFactory.getLogger(Acceptor.class);
//		
//		public Acceptor(EndPoint endPoint, int sendBufferSize, int recvBufferSize, int brokerId, 
//				Processor[] processors, ConnectionQuotas connectionQuotas) throws IOException {
//			super(connectionQuotas);
//			this.endPoint = endPoint;
//			this.sendBufferSize = sendBufferSize;
//			this.recvBufferSize = recvBufferSize;
//			this.brokerId = brokerId;
//			this.processors = processors;
//			
//			serverChannel = openServerSocket(endPoint.host, endPoint.port);
//			selector = Selector.open();
//		}
//		
//		public void run(){
//			try {
//				serverChannel = ServerSocketChannel.open();
//				serverChannel.configureBlocking(false);
//				serverChannel.socket().bind(new InetSocketAddress(endPoint.port));
//				serverChannel.register(selector, SelectionKey.OP_READ);
//				
//				logger.info("Awaiting connections on port " + endPoint.port);
//				startupCompleted();
//				
//				int currentProcessor = 0;
//				while(isRunning()){
//					int ready = selector.select(500);
//					if(ready > 0){
//						Set<SelectionKey> keys = selector.selectedKeys();
//						Iterator<SelectionKey> iter = keys.iterator();
//						while(iter.hasNext() && isRunning()){
//							SelectionKey key = null;
//							try {
//								key = iter.next();
//								iter.remove();
//								if(key.isAcceptable()){
//									accept(key, processors[currentProcessor]);
//								}else{
//									throw new IllegalStateException("Unrecognized key state for acceptor thread");
//								}
//								
//								currentProcessor = (currentProcessor + 1) % processors.length;
//							} catch (Exception e) {
//								logger.error("", e);
//							}
//						}
//					}
//				}
//				
//				serverChannel.close();
//				selector.close();
//				shutdownComplete();
//			} catch (ClosedChannelException e) {
//				e.printStackTrace();
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//		}
//		
//		private ServerSocketChannel openServerSocket(String host, int port) {
//			InetSocketAddress socketAddress = (host == null || host.trim().isEmpty()) ? 
//											new InetSocketAddress(port) : new InetSocketAddress(host, port);
//			try {
//				serverChannel = ServerSocketChannel.open();
//				serverChannel.configureBlocking(false);
//				serverChannel.socket().setReceiveBufferSize(recvBufferSize);
//				serverChannel.socket().bind(socketAddress);
//				logger.info(String.format("Awaiting socket connections on %s:%d.", 
//						socketAddress.getHostString(), 
//						serverChannel.socket().getLocalPort()));
//			} catch (IOException e) {
//				throw new KafkaException(String.format("Socket server failed to bind to %s:%d: %s.", 
//								socketAddress.getHostString(), port, e.getMessage()), e);
//			}
//			return serverChannel;
//			
//		}
//		
//		private void accept(SelectionKey key, Processor processor) throws Exception{
//			ServerSocketChannel ssc = (ServerSocketChannel) key.channel();
//			ssc.socket().setReceiveBufferSize(recvBufferSize);
//			
//			SocketChannel socketChannel = ssc.accept();
//			socketChannel.configureBlocking(false);
//			socketChannel.socket().setTcpNoDelay(true);
//			socketChannel.socket().setSendBufferSize(sendBufferSize);
//			
//			if(logger.isDebugEnabled()){
//				logger.debug("sendBufferSize:[" + socketChannel.socket().getSendBufferSize() + "] receiveBufferSize:[" +
//						socketChannel.socket().getReceiveBufferSize() + "]");
//			}
//			processor.accept(socketChannel);
//		}
//
//		@Override
//		public void wakeup() {
//			// TODO Auto-generated method stub
//			
//		}
//	}
//	
//	private class Processor extends AbstractServerThread{
//		private int id;
//		private int maxRequestSize;
////		private Requestch
//		private long connectionsMaxIdleMs;
//		private SecurityProtocol protocol;
//		private Map<String, ?> channelConfigs;
//		private Metrics metrics;
//		private ConcurrentLinkedQueue<SocketChannel> newConnections;
////		private Map<String, reqye>
//		
//		public Processor(int id, int maxRequestSize, ConnectionQuotas connectionQuotas,
//						long connectionsMaxIdleMs, SecurityProtocol protocol, Map<String, ?> channelConfigs,
//						Metrics metrics) {
//			super(connectionQuotas);
//			this.id = id;
//			this.maxRequestSize = maxRequestSize;
//			this.connectionsMaxIdleMs = connectionsMaxIdleMs;
//			this.protocol = protocol;
//			this.channelConfigs = channelConfigs;
//			this.metrics = metrics;
//		}
//
//		private void accept(SocketChannel socketChannel){
////			newConnections.add(socketChannel);
//			System.out.println("socketChannel accept!");
//		}
//
//		@Override
//		public void run() {
//			// TODO Auto-generated method stub
//			
//		}
//
//		@Override
//		public void wakeup() {
//			// TODO Auto-generated method stub
//			
//		}
//		
//	}
//	
//	private class ConnectionQuotas{
//		
//	}
//	
//	private abstract class AbstractServerThread implements Runnable{
//		private ConnectionQuotas connectionQuotas;
//		private Logger logger = LoggerFactory.getLogger(AbstractServerThread.class);
//		
//		public AbstractServerThread(ConnectionQuotas connectionQuotas){
//			this.connectionQuotas = connectionQuotas;
//		}
//		
//		private CountDownLatch startupLatch = new CountDownLatch(1);
//		private CountDownLatch shutdownLatch = new CountDownLatch(1);
//		private AtomicBoolean alive = new AtomicBoolean(true);
//		
//		public abstract void wakeup();
//		
//		public void alive() throws InterruptedException{
//			alive.set(false);
//			wakeup();
//			shutdownLatch.await();
//		}
//		
//		public void awaitStartup() throws InterruptedException{
//			startupLatch.await();
//		}
//		
//		protected void startupCompleted(){
//			startupLatch.countDown();
//		}
//		
//		protected void shutdownComplete(){
//			shutdownLatch.countDown();
//		}
//		
//		protected boolean isRunning(){
//			return alive.get();
//		}
//		
////		public void close(Selector selector, String connectionId){
////		}
//		
//		public void close(SocketChannel channel){
//			if(channel != null){
//				logger.debug("Closing connection from " + channel.socket().getRemoteSocketAddress());
//				//TODO:
////				connectionQuotas.d
//				try {
//					channel.socket().close();
//					channel.close();
//				} catch (IOException e) {
//					e.printStackTrace();
//				}
//			}
//		}
//	}

}

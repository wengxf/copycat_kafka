package org.apache.kafka.network;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.cluster.EndPoint;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ChannelBuilders;
import org.apache.kafka.common.network.KafkaChannel;
import org.apache.kafka.common.network.LoginType;
import org.apache.kafka.common.network.Mode;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.network.streams.processor.RequestChannel;
import org.apache.kafka.server.KafkaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yammer.metrics.core.Gauge;

/**
 * nio socket服务器，线程模型是：1个Acceptor线程处理新连接，Acceptor还有多个线程器线程，
 * 每个处理器线程拥有自己的selector和多个读socket请求Handler处理。handler线程处理请求并
 * 产生响应写给处理器线程
 * @author baodekang
 *
 */
public class SocketServer extends KafkaMetricsGroup {
	
	private static final Logger logger = LoggerFactory.getLogger(SocketServer.class);
	
	private Map<SecurityProtocol, EndPoint> endpoints;
	private int numProcessorThreads;
	private int maxQueuedRequests;
	private int totalProcessorThreads;
	private int maxConnectionsPerIp;
	private Map<String, Integer> maxConnectionsPerIpOverrides;
	private String logIdent;
	public RequestChannel requestChannel;
	private Processor[] processors;
	private Map<EndPoint, Acceptor> acceptors;
	private ConnectionQuotas connectionQuotas;
	private KafkaConfig config;
	private Metrics metrics;
	private Time time;
	
	public SocketServer(KafkaConfig config, Metrics metrics, Time time) {
		this.config = config;
		this.metrics = metrics;
		this.time = time;
		numProcessorThreads = config.NumNetworkThreads;
		maxQueuedRequests = config.queuedMaxRequests;
		endpoints = config.listeners;
		totalProcessorThreads = numProcessorThreads * endpoints.size();
		maxConnectionsPerIp = config.maxConnectionsPerIp;
		maxConnectionsPerIpOverrides = new HashMap<>();
		logIdent = "[Socket Server on Broker " + config.brokerId + "], ";
		requestChannel = new RequestChannel(totalProcessorThreads, maxQueuedRequests);
		processors = new Processor[totalProcessorThreads];
		acceptors = new HashMap<>();
	}
	
	public void startup(){
		synchronized(this){
			connectionQuotas = new ConnectionQuotas(maxConnectionsPerIp, maxConnectionsPerIpOverrides);
			int sendBufferSize = config.SocketSendBufferBytes;
			int recvBufferSize = config.SocketReceiveBufferBytes;
			int brokerId = config.brokerId;
			
			int processorBeginIndex = 0;
			for(EndPoint endPoint : endpoints.values()){
				SecurityProtocol protocol = endPoint.protocolType;
				int processorEndIndex = processorBeginIndex + numProcessorThreads;
				for(int i = processorBeginIndex; i < processorEndIndex; i++){
					processors[i] = newProcessor(i, connectionQuotas, protocol);
				}
				try {
					Acceptor acceptor = new Acceptor(endPoint, sendBufferSize, recvBufferSize, brokerId,
							Arrays.copyOfRange(processors, processorBeginIndex, processorEndIndex), connectionQuotas);
					acceptors.put(endPoint, acceptor);
					Utils.newThread("kafka-socket-acceptor-%s-%d".format(protocol.toString(), endPoint.port), acceptor, false).start();
					acceptor.awaitStartup();
					
					processorBeginIndex = processorEndIndex;
				} catch (IOException e) {
					e.printStackTrace();
				}
				
			}
		}
	}
	
	public void shutdown(){
		synchronized(this){
			for(Acceptor acceptor : acceptors.values()){
				acceptor.shutdown();
			}
			for(Processor processor : processors){
				processor.shutdown();
			}
			
			logger.info("Shutdown completed");
		}
	}
	
	public int boundPort(SecurityProtocol protocol){
		if(protocol == null){
			protocol = SecurityProtocol.PLAINTEXT;
		}
		return acceptors.get(endpoints.get(protocol)).serverChannel.socket().getLocalPort();
	}
	
	protected Processor newProcessor(int id, ConnectionQuotas connectionQuotas, SecurityProtocol protocol){
		return new Processor(id, time, config.SocketRequestMaxBytes, requestChannel, connectionQuotas,
				config.connectionsMaxIdleMs, protocol, config.values(), metrics);
	}
	
	private abstract class AbstractServerThread implements Runnable{
		private ConnectionQuotas connectionQuotas;
		private CountDownLatch startupLatch;
		private CountDownLatch shutdownLatch;
		private AtomicBoolean alive;
		
		public AbstractServerThread(ConnectionQuotas connectionQuotas) {
			this.connectionQuotas = connectionQuotas;
			startupLatch = new CountDownLatch(1);
			shutdownLatch = new CountDownLatch(1);
			alive = new AtomicBoolean(true);
		}
		
		public abstract void wakeup();
		
		public void shutdown(){
			try {
				alive.set(false);
				wakeup();
				shutdownLatch.wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		public void awaitStartup(){
			try {
				startupLatch.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		protected void startupComplete(){
			startupLatch.countDown();
		}
		
		protected void shutdownComplete(){
			shutdownLatch.countDown();
		}
		
		protected boolean isRunning(){
			return alive.get();
		}
		
		public void close(Selector selector, String connectionId){
			KafkaChannel channel = selector.channel(connectionId);
			if(channel != null){
				logger.debug("Closing selector connection " + connectionId);
				InetAddress address = channel.socketAddress();
				if(address != null){
					connectionQuotas.dec(address);
				}
				selector.close(connectionId);
			}
		}
		
		public void close(SocketChannel channel){
			logger.debug("Closing connection from " + channel.socket().getRemoteSocketAddress());
			connectionQuotas.dec(channel.socket().getInetAddress());
			
		}
	}
	
	private class Processor extends AbstractServerThread{
		
		public int id;
		private Time time;
		private int maxRequestSize;
		private RequestChannel requestChannel;
		private long connectionsMaxIdleMs;
		private SecurityProtocol protocol;
		private Map<String, ?> channelConfigs;
		private Metrics metrics;
		
		private ConcurrentLinkedQueue<SocketChannel> newConnections;
		private Map<String, RequestChannel.Response> inflightResponses;
		private Map<String, String> metricTags;
		
		private Selector selector;
		
		public Processor(int id, Time time, int maxRequestSize, RequestChannel requestChannel, ConnectionQuotas connectionQuotas,
				long connectionsMaxIdleMs, SecurityProtocol protocol, Map<String, ?> channelConfigs, final Metrics metrics) {
			super(connectionQuotas);
			this.id = id;
			this.time = time;
			this.maxRequestSize = maxRequestSize;
			this.requestChannel = requestChannel;
			this.connectionsMaxIdleMs = connectionsMaxIdleMs;
			this.protocol = protocol;
			this.channelConfigs = channelConfigs;
			this.metrics = metrics;
			metricTags = new HashMap<>();
			metricTags.put("networkProcessor", String.valueOf(id));
			
			newConnections = new ConcurrentLinkedQueue<>();
			inflightResponses = new HashMap<>();
			
			newGauge("IdlePercent", new Gauge<Double>() {
				@Override
				public Double value() {
					return metrics.metrics().get(metrics.metricName("io-wait-ratio", "socket-server-metrics", metricTags)).value();
				}
			}, metricTags);
			
			selector = new Selector(maxRequestSize, connectionsMaxIdleMs, metrics, time,
							"socket-server", metricTags, false, 
							ChannelBuilders.create(protocol, Mode.SERVER, 
									LoginType.SERVER, 
									channelConfigs, null, true));
		}
		
		private class ConnectionId{
			private String localHost;
			private int localPort;
			private String remoteHost;
			private int remotePort;
			
			public ConnectionId(String localHost, int localPort, String remoteHost, int remotePort) {
				this.localHost = localHost;
				this.localPort = localPort;
				this.remoteHost = remoteHost;
				this.remotePort = remotePort;
			}
			
			@Override
			public String toString() {
				return localHost + ":" + localPort + "-" + remoteHost + ":" + remotePort;
			}
//			public static void fromString(String s){
//				String[] array = s.split("-");
//				broker
//			}
		}
		
		
		public Processor(ConnectionQuotas connectionQuotas) {
			super(connectionQuotas);
		}

		@Override
		public void run() {
			startupComplete();
			while(true){
				//setup any new connections that have been queued up
				try {
					configureNewConnections();
					processNewResponses();
					poll();
					processCompletedReceives();
					processCompletedSends();
					processDisconnected();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		@Override
		public void wakeup() {
			
		}
		
		private void configureNewConnections() throws IOException{
			while(!newConnections.isEmpty()){
				SocketChannel channel = newConnections.poll();
				
				try {
					logger.debug(String.format("Processor %s listening to new connection from %s", id, 
								channel.socket().getRemoteSocketAddress()));
					
					String localHost = channel.socket().getLocalAddress().getHostAddress();
					int localPort = channel.socket().getLocalPort();
					String remoteHost = channel.socket().getInetAddress().getHostAddress();
					int remotePort = channel.socket().getPort();
					
					String connectionId = new ConnectionId(localHost, localPort, remoteHost, remotePort).toString();
					selector.register(connectionId, channel);
				} catch (ClosedChannelException e) {
					close(channel);
					logger.error(String.format("Processor %s closed connection from %s", id, channel.getRemoteAddress()));
				}
			}
		}
		
		private void processNewResponses(){
//			int curr = requestChannel.rec
		}
		
		private void poll(){
			
		}
		
		private void processCompletedReceives(){
			
		}
		
		private void processCompletedSends(){
			
		}
		
		private void processDisconnected(){
			
		}
		
		private void accept(SocketChannel socketChannel){
			newConnections.add(socketChannel);
			wakeup();
		}
	}
	
	private class Acceptor extends AbstractServerThread{
		private int sendBufferSize;
		private int recvBufferSize;
		private java.nio.channels.Selector nioSelector;
		private ServerSocketChannel serverChannel;
		
		public Acceptor(EndPoint endPoint, int sendBufferSize, int recvBufferSize, int brokerId, 
				Processor[] processors, ConnectionQuotas connectionQuotas) throws IOException {
			super(connectionQuotas);
			this.sendBufferSize = sendBufferSize;
			this.recvBufferSize = recvBufferSize;
			synchronized(this){
				for(Processor processor : processors){
					Utils.newThread(String.format("kafka-network-thread-%d-%s-%d", 
								brokerId, endPoint.protocolType.toString(),
								processor.id), processor, false);
				}
			}
			this.nioSelector = java.nio.channels.Selector.open();
			this.serverChannel = openServerSocket(endPoint.host, endPoint.port);
		}
		
		private ServerSocketChannel openServerSocket(String host, int port){
			InetSocketAddress address = (host == null || host.trim().isEmpty()) ? new InetSocketAddress(port) : 
				 						new InetSocketAddress(host, port);
			ServerSocketChannel channel = null;
			try {
				channel = ServerSocketChannel.open();
				channel.configureBlocking(false);
				channel.socket().setReceiveBufferSize(recvBufferSize);
				channel.socket().bind(address);
				
				logger.info(String.format("Awaiting socket connections on %s:%d", address.getHostString(), channel.socket().getLocalPort()));
			} catch (SocketException e) {
				throw new KafkaException("Socket server failed to bind to %s:%d: %s.".format(address.getHostString(), port, e.getMessage()), e);
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			return channel;
		}

		@Override
		public void run() {
			try {
				serverChannel.register(nioSelector, SelectionKey.OP_ACCEPT);
				startupComplete();
				
				int currentProcessor = 0;
				while(isRunning()){
					int ready = nioSelector.select(500);
					if(ready > 0){
						Iterator<SelectionKey> iter = nioSelector.selectedKeys().iterator();
						while(iter.hasNext() && isRunning()){
							SelectionKey key = iter.next();
							iter.remove();
							if(key.isAcceptable()){
								accept(key, processors[currentProcessor]);
							}else{
								throw new IllegalStateException();
							}
							currentProcessor = (currentProcessor + 1) % processors.length;
						}
					}
				}
			} catch (ClosedChannelException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally{
				logger.debug("Closing server socket and selector.");
				try {
					serverChannel.close();
					nioSelector.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
				shutdownComplete();
			}
		}
		
		public void accept(SelectionKey key, Processor processor){
			ServerSocketChannel channel = (ServerSocketChannel) key.channel();
			SocketChannel socketChannel = null;
			try {
				socketChannel = channel.accept();
				connectionQuotas.inc(socketChannel.socket().getInetAddress());
				socketChannel.configureBlocking(false);
				socketChannel.socket().setTcpNoDelay(true);
				socketChannel.socket().setKeepAlive(true);
				socketChannel.socket().setSendBufferSize(sendBufferSize);
				
				logger.debug(String.format("Accepted connection from %s on %s. sendBufferSize [actual|requested]: [%d|%d] recvBufferSize [actual|requested]: [%d|%d]", 
						socketChannel.socket().getInetAddress(), 
						socketChannel.socket().getLocalSocketAddress(),
		                socketChannel.socket().getSendBufferSize(), 
		                sendBufferSize,
		                socketChannel.socket().getReceiveBufferSize(), 
		                recvBufferSize));
				
				processor.accept(socketChannel);
			} catch (IOException e) {
				close(socketChannel);
				e.printStackTrace();
			}
		}

		@Override
		public void wakeup() {
			this.nioSelector.wakeup();
		}
		
	}
	
	private class ConnectionQuotas{
		private int defaultMax;
		private Map<InetAddress, Integer> overrides;
		private Map<InetAddress, Integer> counts;
		
		public ConnectionQuotas(Integer defaultMax, Map<String, Integer> overrideQuotas) {
			try {
				this.defaultMax = defaultMax;
				overrides = new HashMap<>();
				for(String host : overrideQuotas.keySet()){
					overrides.put(InetAddress.getByName(host), overrideQuotas.get(host));
				}
				counts = new HashMap<>();
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
		}
		
		public void inc(InetAddress address){
			synchronized(counts){
				int count = 0;
				if(!counts.isEmpty()){
					count = counts.get(address);
				}
				counts.put(address, count + 1);
				
				if(count >= defaultMax){
					throw new TooManyConnectionsException(address, count);
				}
			}
		}
		
		public void dec(InetAddress address){
			synchronized(counts){
				if(counts.isEmpty() || !counts.containsKey(address)){
					throw new IllegalArgumentException(String.format("Attempted to decrease connection count for address with no connections, address:", address));
				}
				int count = counts.get(address);
				if(count == 1){
					counts.remove(address);
				}else{
					counts.put(address, count - 1);
				}
			}
		}
		
		public int get(InetAddress address){
			synchronized (counts) {
				return (counts.isEmpty() || !counts.containsKey(address)) ? 0 : counts.get(address);
			}
		}
	}
	
	private class TooManyConnectionsException extends KafkaException{
		public TooManyConnectionsException(InetAddress ip, int count) {
			super(String.format("Too many connections from %s (maximum = %d)", ip, count));
		}
	}

}

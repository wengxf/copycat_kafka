package org.apache.kafka.server.network;

import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.NetworkSend;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.network.SocketServer;
import org.apache.kafka.network.streams.processor.RequestChannel;
import org.apache.kafka.server.KafkaConfig;
import org.junit.Before;
import org.junit.Test;

public class SocketServerTest {

	private Properties props = null;
	private SocketServer server;
	private List<Socket> sockets = new ArrayList<>();
	
	@Before
	public void setup(){
		props = SocketServerTest.getPropsFromArgs();
		props.put("listeners", "PLAINTEXT://localhost:0,TRACE://localhost:0");
		props.put("num.network.threads", "1");
		props.put("socket.send.buffer.bytes", "300000");
		props.put("socket.receive.buffer.bytes", "300000");
		props.put("queued.max.requests", "50");
		props.put("socket.request.max.bytes", "50");
		props.put("max.connections.per.ip", "5");
		props.put("connections.max.idle.ms", "60000");
		
		KafkaConfig config = KafkaConfig.fromProps(props);
		Metrics metrics = new Metrics();
		server = new SocketServer(config, metrics, new SystemTime());
		server.startup();
	}
	
	private Socket connect(SecurityProtocol protocol){
		try {
			protocol = protocol == null ? SecurityProtocol.PLAINTEXT : protocol;
			Socket socket = new Socket("localhost", server.boundPort(protocol));
			sockets.add(socket);
			
			return socket;
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return null;
	}
	
	private void sendRequest(Socket socket, byte[] requests, Short id){
		try {
			DataOutputStream outgoing = new DataOutputStream(socket.getOutputStream());
			if(id == null){
				outgoing.writeInt(requests.length);
			}else{
				outgoing.writeInt(requests.length + 2);
				outgoing.writeShort(id);
			}
			
			outgoing.write(requests);
			outgoing.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private byte[] producerRequestBytes(){
		Short apiKey = (short)0;
		int correlationId = -1;
		String clientId = "";
		int ackTimeoutMs = 10000;
		Short ack = (short)0;
		RequestHeader emptyHeader = new RequestHeader(apiKey, clientId, correlationId);
		ProduceRequest emptyRequest = new ProduceRequest(ack, ackTimeoutMs, new HashMap<TopicPartition, ByteBuffer>());
		
		ByteBuffer bytebuffer = ByteBuffer.allocate(emptyHeader.sizeOf() + emptyRequest.sizeOf());
		emptyHeader.writeTo(bytebuffer);
		emptyRequest.writeTo(bytebuffer);
		bytebuffer.rewind();
		byte[] serializedBytes = new byte[bytebuffer.remaining()];
		bytebuffer.get(serializedBytes);
		return serializedBytes;
	}
	
	private void processRequest(RequestChannel channel){
		try {
			RequestChannel.Request request = channel.receiveRequest(2000);
			processRequest(channel, request);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	private void processRequest(RequestChannel channel, RequestChannel.Request request){
		ByteBuffer byteBuffer = ByteBuffer.allocate(request.header.sizeOf() + request.body.sizeOf());
		request.header.writeTo(byteBuffer);
		request.body.writeTo(byteBuffer);
		byteBuffer.rewind();
		
		NetworkSend send = new NetworkSend(request.connectionId, byteBuffer);
		channel.sendResponse(new RequestChannel.Response(request.processor, request, send));
	}
	
	private static Properties getPropsFromArgs(){
		try {
			return Utils.loadProps("server.properties");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return null;
	}
	
	@Test
	public void simpleRequest(){
		Socket plainSocket = connect(SecurityProtocol.PLAINTEXT);
		byte[] serializedBytes = producerRequestBytes();
		sendRequest(plainSocket, serializedBytes, null);
		processRequest(server.requestChannel);
	}
}

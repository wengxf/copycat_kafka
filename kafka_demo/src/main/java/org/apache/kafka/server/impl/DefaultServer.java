package org.apache.kafka.server.impl;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.server.Server;

/**
 * 服务注册类
 * @author baodekang
 *
 */
public class DefaultServer implements Server{

	private static ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
	
	private static final HashMap<String, Class<?>> serviceRegistry = new HashMap<String, Class<?>>();
	
	private static boolean isRunning = false;
	
	private static int port;
	
	public DefaultServer(int port) {
		this.port = port;
	}
	
	@Override
	public void stop() {
		isRunning = false;
		executor.shutdown();
	}

	@Override
	public void start() throws IOException {
		ServerSocket server = new ServerSocket();
		server.bind(new InetSocketAddress(port));
		System.out.println("start server");
		try {
			while(true){
				executor.execute(new ServiceTask(server.accept()));
			}
		} finally{
			server.close();
		}
	}

	@Override
	public void register(Class<?> serviceInterface, Class<?> impl) {
		serviceRegistry.put(serviceInterface.getName(), impl);
	}

	@Override
	public boolean isRunning() {
		return isRunning;
	}

	@Override
	public int getPort() {
		return port;
	}
	
	private static class ServiceTask implements Runnable{

		private Socket client = null;
		
		public ServiceTask(Socket client) {
			this.client = client;
		}
		
		@Override
		public void run() {
			ObjectInputStream input = null;
			ObjectOutputStream output = null;
			
			try {
				input = new ObjectInputStream(client.getInputStream());
				String serviceName = input.readUTF();
				String methodName = input.readUTF();
				Class<?>[] parameterTypes = (Class<?>[]) input.readObject();
				Object[] arguments = (Object[]) input.readObject();
				
				Class<?> serviceClass = serviceRegistry.get(serviceName);
				if(serviceClass == null){
					throw new ClassNotFoundException(serviceName + " not found");
				}
				Method method = serviceClass.getMethod(methodName, parameterTypes);
				Object result = method.invoke(serviceClass.newInstance(), arguments);
				
				output = new ObjectOutputStream(client.getOutputStream());
				output.writeObject(result);
			} catch (Exception e) {
				e.printStackTrace();
			} finally{
				if(output != null){
					try {
						output.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				
				if(input != null){
					try {
						input.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				
				if(client != null){
					try {
						client.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}
}

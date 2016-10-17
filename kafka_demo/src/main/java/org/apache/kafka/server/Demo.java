package org.apache.kafka.server;

import java.net.InetSocketAddress;

import org.apache.kafka.server.impl.RPCClient;

public class Demo {

	public static void main(String[] args) {
		for(int i=0; i< 100000; i++){
			HelloService service = RPCClient.getRemoteProxyObj(HelloService.class, new InetSocketAddress("localhost", 8088));
			System.out.println(service.sayHi("zhangshan" + i));
		}
		
	}
}

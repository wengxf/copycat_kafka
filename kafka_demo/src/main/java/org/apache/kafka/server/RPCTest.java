package org.apache.kafka.server;

import java.io.IOException;

import org.apache.kafka.server.impl.DefaultServer;

public class RPCTest {

	public static void main(String[] args) throws IOException {
		Server serviceServer = new DefaultServer(8088);
		serviceServer.register(HelloService.class, HelloSerivceImpl.class);
		serviceServer.start();
	}
}

package org.apache.kafka.server;

public class HelloSerivceImpl implements HelloService{

	@Override
	public String sayHi(String name) {
		return "hi, " + name;
	}

}

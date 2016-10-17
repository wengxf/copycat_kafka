package org.apache.kafka.network.streams.processor;

import java.io.IOException;

public interface Receive {

	public String source();
	
	public boolean complete();
	
	public long readFrom() throws IOException;
}

package org.apache.kafka.util.utils;

import java.io.File;
import java.util.Properties;

import org.apache.kafka.common.protocol.SecurityProtocol;

public class TestUtils {

	public static int MockZkPort = 2181;
	public static String MockZkConnect = "127.0.0.1:" + 2181;
	
	
	public static Properties createBrokerConfig(int nodeId, String zkConnect, int port){
		Properties props = new Properties();
		if (nodeId >= 0) props.put("broker.id", String.valueOf(nodeId));
//	    props.put("listeners", listeners);
//	    props.put("log.dir", TestUtils.tempDir().getAbsolutePath);
//	    props.put("zookeeper.connect", zkConnect);
//	    props.put("replica.socket.timeout.ms", "1500");
//	    props.put("controller.socket.timeout.ms", "1500");
//	    props.put("controlled.shutdown.enable", enableControlledShutdown.toString);
//	    props.put("delete.topic.enable", enableDeleteTopic.toString);
//	    props.put("controlled.shutdown.retry.backoff.ms", "100");
//	    props.put("log.cleaner.dedupe.buffer.size", "2097152");
//	    rack.foreach(props.put("broker.rack", _));
//
//	    if (protocolAndPorts.exists { case (protocol, _) => usesSslTransportLayer(protocol) })
//	      props.putAll(sslConfigs(Mode.SERVER, false, trustStoreFile, s"server$nodeId"))
//
//	    if (protocolAndPorts.exists { case (protocol, _) => usesSaslTransportLayer(protocol) })
//	      props.putAll(saslConfigs(saslProperties))
//
//	    interBrokerSecurityProtocol.foreach { protocol =>
//	      props.put(KafkaConfig.InterBrokerSecurityProtocolProp, protocol.name)
//	    }
//
//	    props.put("port", port.toString)
	    return props;
	}
}

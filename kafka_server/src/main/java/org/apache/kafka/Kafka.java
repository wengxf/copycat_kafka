package org.apache.kafka;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.KafkaServerStartable;

/**
 * 该类为kafka broker的main启动类，
 * 其主要作用为加载配置：启动report服务（内部状态的监控）
 * 				        注册释放资源的钩子
 * 				        以及门面入口类
 * @author baodekang
 *
 */
public class Kafka {

	public static void main(String[] args) {
		Properties serverProps = getPropsFromArgs();
		final KafkaServerStartable kafkaServerStartable= KafkaServerStartable.fromProps(serverProps);
		Runtime.getRuntime().addShutdownHook(new Thread(){
			@Override
			public void run() {
				kafkaServerStartable.shutdown();
			}
		});
		kafkaServerStartable.startup();
		kafkaServerStartable.awaitShutdown();
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
}

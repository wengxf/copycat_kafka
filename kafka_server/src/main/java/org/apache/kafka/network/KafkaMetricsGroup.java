package org.apache.kafka.network;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;

public class KafkaMetricsGroup {

	private MetricName metricName(String name, Map<String, String> tags){
		Class klass = this.getClass();
		String pkg = klass.getPackage() == null ? "" : klass.getPackage().getName();
		String simpleName = klass.getSimpleName().replaceAll("\\$$", "");
		
		return explicitMetricName(pkg, simpleName, name, tags);
	}
	
	private MetricName explicitMetricName(String group, String typeName, String name, Map<String, String> tags){
		StringBuilder nameBuilder = new StringBuilder();
		nameBuilder.append(group);
		nameBuilder.append(":type=");
		nameBuilder.append(typeName);
		
		if(name.length() > 0){
			nameBuilder.append(",name=");
			nameBuilder.append(name);
		}
		
		String scope = KafkaMetricsGroup.toScope(tags);
		String tagsName = KafkaMetricsGroup.toMBeanName(tags);
		return new MetricName(group, typeName, name, scope, nameBuilder.toString());
	}
	
	private static List<MetricName> consumerMetricNameList(){
		List<MetricName> list = new ArrayList<>();
		list.add(new MetricName("kafka.consumer", "ZookeeperConsumerConnector", "FetchQueueSize"));
		list.add(new MetricName("kafka.consumer", "ZookeeperConsumerConnector", "KafkaCommitsPerSec"));
		list.add(new MetricName("kafka.consumer", "ZookeeperConsumerConnector", "ZooKeeperCommitsPerSec"));
		list.add(new MetricName("kafka.consumer", "ZookeeperConsumerConnector", "RebalanceRateAndTime"));
		list.add(new MetricName("kafka.consumer", "ZookeeperConsumerConnector", "OwnedPartitionsCount"));
		list.add(new MetricName("kafka.consumer", "ConsumerFetcherManager", "MaxLag"));
		list.add(new MetricName("kafka.consumer", "ConsumerFetcherManager", "MinFetchRate"));

	    // kafka.server.AbstractFetcherThread <-- kafka.consumer.ConsumerFetcherThread
		list.add(new MetricName("kafka.server", "FetcherLagMetrics", "ConsumerLag"));

	    // kafka.consumer.ConsumerTopicStats <-- kafka.consumer.{ConsumerIterator, PartitionTopicInfo}
		list.add(new MetricName("kafka.consumer", "ConsumerTopicMetrics", "MessagesPerSec"));

	    // kafka.consumer.ConsumerTopicStats
		list.add(new MetricName("kafka.consumer", "ConsumerTopicMetrics", "BytesPerSec"));

	    // kafka.server.AbstractFetcherThread <-- kafka.consumer.ConsumerFetcherThread
		list.add(new MetricName("kafka.server", "FetcherStats", "BytesPerSec"));
		list.add(new MetricName("kafka.server", "FetcherStats", "RequestsPerSec"));

	    // kafka.consumer.FetchRequestAndResponseStats <-- kafka.consumer.SimpleConsumer
		list.add(new MetricName("kafka.consumer", "FetchRequestAndResponseMetrics", "FetchResponseSize"));
		list.add(new MetricName("kafka.consumer", "FetchRequestAndResponseMetrics", "FetchRequestRateAndTimeMs"));
		list.add(new MetricName("kafka.consumer", "FetchRequestAndResponseMetrics", "FetchRequestThrottleRateAndTimeMs"));

	    /**
	     * ProducerRequestStats <-- SyncProducer
	     * metric for SyncProducer in fetchTopicMetaData() needs to be removed when consumer is closed.
	     */
		list.add(new MetricName("kafka.producer", "ProducerRequestMetrics", "ProducerRequestRateAndTimeMs"));
		list.add(new MetricName("kafka.producer", "ProducerRequestMetrics", "ProducerRequestSize"));
		
		return list;
	}
	
	private static List<MetricName> producerMetricNameList(){
		List<MetricName> list = new ArrayList<>();
		list.add(new MetricName("kafka.producer", "ProducerStats", "SerializationErrorsPerSec"));
		list.add(new MetricName("kafka.producer", "ProducerStats", "ResendsPerSec"));
		list.add(new MetricName("kafka.producer", "ProducerStats", "FailedSendsPerSec"));
		
		list.add(new MetricName("kafka.producer.async", "ProducerSendThread", "ProducerQueueSize"));
		list.add(new MetricName("kafka.producer", "ProducerTopicMetrics", "MessagesPerSec"));
		list.add(new MetricName("kafka.producer", "ProducerTopicMetrics", "DroppedMessagesPerSec"));
		list.add(new MetricName("kafka.producer", "ProducerTopicMetrics", "BytesPerSec"));

	    // kafka.server.AbstractFetcherThread <-- kafka.consumer.ConsumerFetcherThread
		list.add(new MetricName("kafka.producer", "ProducerRequestMetrics", "ProducerRequestRateAndTimeMs"));

	    // kafka.consumer.ConsumerTopicStats <-- kafka.consumer.{ConsumerIterator, PartitionTopicInfo}
		list.add(new MetricName("kafka.producer", "ProducerRequestMetrics", "ProducerRequestSize"));

	    // kafka.consumer.ConsumerTopicStats
		list.add(new MetricName("kafka.producer", "ProducerRequestMetrics", "ProducerRequestThrottleRateAndTimeMs"));
		return list;
	}
	
	private static String toMBeanName(Map<String, String> tags){
		String result = null;
		String division = "";
		for(String key : tags.keySet()){
			if(key != null && !key.trim().isEmpty()){
				result = division + result + String.format("%s=%s", key, tags.get(key));
				division = ",";
			}
		}
		return result;
	}
	
	private static String toScope(Map<String, String> tags){
		String result = null;
		String division = "";
		for(String key : tags.keySet()){
			if(key != null && !key.trim().isEmpty()){
				result = division + result + String.format("%s=%s", key, tags.get(key).replace("\\.", "_"));
				division = ".";
			}
		}
		return result;
	}
	
	public void newGauge(String name, Gauge metric,Map<String, String> tags){
		Metrics.defaultRegistry().newGauge(metricName(name, tags), metric);
	}
	
	private static void removeAllConsumerMetrics(String clientId){
//		FetchRequestAndResponseStatsRegistry
	}
	
//	public static void main(String[] args) {
//		KafkaMetricsGroup group = new KafkaMetricsGroup();
//		group.metricName(null, null);
//	}
}

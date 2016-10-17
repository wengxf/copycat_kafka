package org.apache.kafka.clients.producer;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Properties props = new Properties();
 * props.put("bootstrap.servers", "localhost:9092");
 * props.put("acks", "all");
 * props.put("retries", 0);
 * props.put("batch.size", 16384);
 * props.put("linger.ms", 1);
 * props.put("buffer.memory", 33554432);
 * props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
 * props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
 *
 * Producer<String, String> producer = new KafkaProducer<>(props);
 * for(int i = 0; i < 100; i++)
 *     producer.send(new ProducerRecord<String, String>("my-topic", Integer.toString(i), Integer.toString(i)));
 *
 * producer.close();
 * @author baodekang
 *
 * @param <K>
 * @param <V>
 */
public class KafkaProducer<K, V> implements Producer<K, V>{
	
	private static final Logger logger = LoggerFactory.getLogger(KafkaProducer.class);
	private static final AtomicInteger PRODUCER_CLIENT_ID_SEQUENCE = new AtomicInteger(1);
	private static final String JMX_PREFIX = "kafka.producer";
	
	private String clientId;
	private final Partitioner partitioner;
	private final int maxRequestSize;
	private final long totalMemorySize;
	private final Metadata metadata;
	
	private final Metrics metrics;
	private final CompressionType compressionType;
	private final Time time;
	private final Serializer<K> keySerializer;
	private final Serializer<V> valueSerializer;
	private final ProducerConfig producerConfig;
	private final long maxBlockTimeMs;
//	private final int requestTimeoutMs;
	private final ProducerInterceptor<K, V> interceptors;
	
	public KafkaProducer(Map<String, Object> configs) {
		this(new ProducerConfig(configs), null, null);
	}
	
	public KafkaProducer(Map<String, Object> configs, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
		this(new ProducerConfig(ProducerConfig.addSerializerToConfig(configs, keySerializer, valueSerializer)),
				keySerializer, valueSerializer);
	}
	
	public KafkaProducer(Properties properties){
		this(new ProducerConfig(properties), null, null);
	}
	
	public KafkaProducer(Properties properties, Serializer<K> keySerializer, Serializer<V> valueSerializer){
		this(new ProducerConfig(ProducerConfig.addSerializerToConfig(properties, keySerializer, valueSerializer)),
				keySerializer, valueSerializer);
	}
	
	private KafkaProducer(ProducerConfig config, Serializer<K> keySerializer, Serializer<V> valueSerializer){
		logger.info("Starting the Kafka producer");
		Map<String, Object> userProvidedConfig = config.originals();
		this.producerConfig = config;
		this.time = new SystemTime();
		
		clientId = config.getString(ProducerConfig.CLIENT_ID_CONFIG);
		if(clientId.length() <= 0){
			clientId = "producer-" + PRODUCER_CLIENT_ID_SEQUENCE.getAndIncrement();
		}
		Map<String, String> metricTags = new LinkedHashMap<String, String>();
		metricTags.put("client-id", clientId);
		MetricConfig metricConfig = new MetricConfig().samples(config.getInt(ProducerConfig.METRICS_NUM_SAMPLES_CONFIG))
                .timeWindow(config.getLong(ProducerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS)
                .tags(metricTags);
		List<MetricsReporter> reporters = config.getConfiguredInstances(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG,
                MetricsReporter.class);
//        reporters.add(new JmxReporter(JMX_PREFIX));
        this.metrics = new Metrics(metricConfig, reporters, time);
        this.partitioner = config.getConfiguredInstance(ProducerConfig.PARTITIONER_CLASS_CONFIG, Partitioner.class);
        long retryBackoffMs = config.getLong(ProducerConfig.RETRY_BACKOFF_MS_CONFIG);
//        this.metadata = new Metadata(retryBackoffMs, config.getLong(ProducerConfig.METADATA_MAX_AGE_CONFIG));
        this.maxRequestSize = config.getInt(ProducerConfig.MAX_REQUEST_SIZE_CONFIG);
        this.totalMemorySize = config.getLong(ProducerConfig.BUFFER_MEMORY_CONFIG);
        this.compressionType = CompressionType.forName(config.getString(ProducerConfig.COMPRESSION_TYPE_CONFIG));
        
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.maxBlockTimeMs = Long.MAX_VALUE;
        
        metadata = new Metadata();
        
        interceptors = null;
        
	}

	@Override
	public Future<RecordMetadata> send(Producer<K, V> record) {
		return null;
	}

	@Override
	public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
		return null;
	}
	
	/**
	 * 异步发送数据到topic
	 * @param record
	 * @param callback
	 * @return
	 */
	private Future<RecordMetadata> doSend(ProducerRecord<K, V> record, Callback callback){
		TopicPartition tp = null;
		//确保metadata可用
		long waitedOnMetadataMs = waitOnMetadata(record.topic(), this.maxBlockTimeMs);
		long remainingWaitMs = Math.max(0, this.maxBlockTimeMs - waitedOnMetadataMs);
		byte[] serializedKey;
		try {
			serializedKey = keySerializer.serialize(record.topic(), record.key());
		} catch (Exception e) {
			throw new SerializationException("Can't convert key of class " + record.key().getClass().getName() +
					" to class " + producerConfig.getClass(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG).getName() + 
					" specified in key.serializer");
		}
		byte[] serializedValue;
		try {
			serializedValue = valueSerializer.serialize(record.topic(), record.value());
		} catch (Exception e) {
			throw new SerializationException("Can't convert value of class " + record.value().getClass().getName() + 
					" to class " + producerConfig.getClass(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG).getName() +
					" specified in value.serializer");
		}
		
		int partition = partition(record, serializedKey, serializedValue, metadata.fetch());
		int serializedSize = Records.LOG_OVERHEAD + Record.recordSize(serializedKey, serializedValue);
		ensureValidRecordSize(serializedSize);
		
		tp = new TopicPartition(record.topic(), partition);
		long timestamp = record.timestamp() == null ? time.milliseconds() : record.timestamp();
		logger.info(String.format("Sending record {} with callback {} to topic {} partition {}", record,
				callback, record.topic(), partition));
//		Callback intercepCallback = interceptors.is
		
		return null;
	}
	
	/**
	 * 验证记录size是否太大
	 * @param size
	 */
	private void ensureValidRecordSize(int size){
		if(size > this.maxRequestSize){
			throw new IllegalArgumentException("This message is " + size  + 
					" bytes when serialized which is larger than maximum request size you have configured with the " +
					ProducerConfig.MAX_REQUEST_SIZE_CONFIG + " configuration");
		}
		if(size > this.totalMemorySize){
			throw new IllegalArgumentException("The message is " + size +
                    " bytes when serialized which is larger than the total memory buffer you have configured with the " +
                    ProducerConfig.BUFFER_MEMORY_CONFIG +
                    " configuration.");
		}
	}
	
	/**
	 * 等待集群对应给定topic的metadata可用
	 * @param topic
	 * @param maxWaitMs
	 * @return
	 */
	private long waitOnMetadata(String topic, long maxWaitMs){
		if(!this.metadata.containsTopic(topic)){
			this.metadata.add(topic);
		}
//		if(metadata.fetch().partitionsForTopic(topic) != null){
//			return 0;
//		}
		
		long begin = System.currentTimeMillis();
//		long remainingWaitMs = maxWaitMs;
//		while()
		return time.milliseconds() - begin;
	}

	@Override
	public void flush() {
		
	}

	@Override
	public List<PartitionInfo> partitionsFor(String topic) {
		return null;
	}

	@Override
	public Map metrics() {
		return null;
	}

	@Override
	public void close() {
		
	}

	@Override
	public void close(long timeout, TimeUnit unit) {
		
	}
	
	/**
	 * 计算给定记录的分区
	 * @param record
	 * @param serializedKey
	 * @param serializedValue
	 * @param cluster
	 * @return
	 */
	private int partition(ProducerRecord<K, V> record, byte[] serializedKey, byte[] serializedValue, Cluster cluster){
		Integer partition = record.partition();
		if(partition != null){
			List<PartitionInfo> partitions = cluster.partitionsForTopic(record.topic());
			int lastPartition = partitions.size() - 1;
			if(partition < 0 || partition > lastPartition){
				throw new IllegalArgumentException(String.format("Invalid partition given with record:"
						+ " %d is not the range [0...%d]", partition, lastPartition));
			}
			
			return partition;
		}
		return this.partitioner.partition(record.topic(), record.key(), serializedKey, record.value(), 
				serializedValue, cluster);
	}

}

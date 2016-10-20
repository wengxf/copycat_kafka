package org.apache.kafka.server;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.cluster.EndPoint;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.protocol.SecurityProtocol;

public class KafkaConfig extends AbstractConfig{

	public Map<String, Object> props;
	
	private KafkaConfig(Map<String, Object> props) {
		super(props);
		this.props = props;
	}
	
	public static class Defaults{
		/** ********* Zookeeper Configuration ***********/
		  public int ZkSessionTimeoutMs = 6000;
		  public int ZkSyncTimeMs = 2000;
		  public boolean ZkEnableSecureAcls = false;

		  /** ********* General Configuration ***********/
		  public boolean BrokerIdGenerationEnable = true;
		  public int MaxReservedBrokerId = 1000;
		  public int BrokerId = -1;
		  public int MessageMaxBytes = 1000000;
		  public static int NumNetworkThreads = 3;
		  public int NumIoThreads = 8;
		  public int BackgroundThreads = 10;
		  public static int QueuedMaxRequests = 500;

		  /************* Authorizer Configuration ***********/
		  public String AuthorizerClassName = "";

		  /** ********* Socket Server Configuration ***********/
		  public int Port = 9092;
		  public String HostName = new String("");
		  public int SocketSendBufferBytes = 100 * 1024;
		  public int SocketReceiveBufferBytes = 100 * 1024;
		  public int SocketRequestMaxBytes = 100 * 1024 * 1024;
		  public static int MaxConnectionsPerIp = Integer.MAX_VALUE;
		  public String MaxConnectionsPerIpOverrides = "";
		  public Long ConnectionsMaxIdleMs = 10 * 60 * 1000L;
		  public int RequestTimeoutMs = 30000;

		  /** ********* Log Configuration ***********/
		  public int NumPartitions = 1;
		  public String LogDir = "/tmp/kafka-logs";
		  public int LogSegmentBytes = 1 * 1024 * 1024 * 1024;
		  public int LogRollHours = 24 * 7;
		  public int LogRollJitterHours = 0;
		  public int LogRetentionHours = 24 * 7;

		  public Long LogRetentionBytes = -1L;
		  public Long LogCleanupInterpublic = 5 * 60 * 1000L;
		  public String Delete = "delete";
		  public String Compact = "compact";
		  public String LogCleanupPolicy = Delete;
		  public int LogCleanerThreads = 1;
		  public Double LogCleanerIoMaxBytesPerSecond = 0d;
		  public Long LogCleanerDedupeBufferSize = 128 * 1024 * 1024L;
		  public int LogCleanerIoBufferSize = 512 * 1024;
		  public Double LogCleanerDedupeBufferLoadFactor = 0.9d;
		  public int LogCleanerBackoffMs = 15 * 1000;
		  public Double LogCleanerMinCleanRatio = 0.5d;
		  public boolean LogCleanerEnable = true;
		  public Long LogCleanerDeleteRetentionMs = 24 * 60 * 60 * 1000L;
		  public int LogIndexSizeMaxBytes = 10 * 1024 * 1024;
		  public int LogIndexInterpublic = 4096;
		  public int LogFlushInterpublic = 0;
		  public int LogDeleteDelayMs = 60000;
		  public int LogFlushSchedulerInterpublic = 0;
		  public int LogFlushOffsetCheckpointInterpublic = 60000;
		  public boolean LogPreAllocateEnable = false;
	}
	
	public String LogConfigPrefix = "log.";
	
	/** ********* Zookeeper Configuration ***********/
  public static final String ZkConnectProp = "zookeeper.connect";
  public static final String ZkSessionTimeoutMsProp = "zookeeper.session.timeout.ms";
  public static final String ZkConnectionTimeoutMsProp = "zookeeper.connection.timeout.ms";
  public static final String ZkSyncTimeMsProp = "zookeeper.sync.time.ms";
  public static final String ZkEnableSecureAclsProp = "zookeeper.set.acl";
  /** ********* General Configuration ***********/;
  public static final String BrokerIdGenerationEnableProp = "broker.id.generation.enable";
  public static final String MaxReservedBrokerIdProp = "reserved.broker.max.id";
  public static final String BrokerIdProp = "broker.id";
  public static final String MessageMaxBytesProp = "message.max.bytes";
  public static final String NumNetworkThreadsProp = "num.network.threads";
  public static final String NumIoThreadsProp = "num.io.threads";
  public static final String BackgroundThreadsProp = "background.threads";
  public static final String QueuedMaxRequestsProp = "queued.max.requests";
//  public static final String RequestTimeoutMsProp = CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG;
  /************* Authorizer Configuration ***********/
  public static final String AuthorizerClassNameProp = "authorizer.class.name";
  /** ********* Socket Server Configuration ***********/
  public static final String PortProp = "port";
  public static final String HostNameProp = "host.name";
  public static final String ListenersProp = "listeners";
  public static final String AdvertisedHostNameProp = "advertised.host.name";
  public static final String AdvertisedPortProp = "advertised.port";
  public static final String AdvertisedListenersProp = "advertised.listeners";
  public static final String SocketSendBufferBytesProp = "socket.send.buffer.bytes";
  public static final String SocketReceiveBufferBytesProp = "socket.receive.buffer.bytes";
  public static final String SocketRequestMaxBytesProp = "socket.request.max.bytes";
  public static final String MaxConnectionsPerIpProp = "max.connections.per.ip";
  public static final String MaxConnectionsPerIpOverridesProp = "max.connections.per.ip.overrides";
  public static final String ConnectionsMaxIdleMsProp = "connections.max.idle.ms";
  /***************** rack configuration *************/
  public static final String RackProp = "broker.rack";
  /** ********* Log Configuration ***********/
  public static final String NumPartitionsProp = "num.partitions";
  public static final String LogDirsProp = "log.dirs";
  public static final String LogDirProp = "log.dir";
  public static final String LogSegmentBytesProp = "log.segment.bytes";

  public static final String LogRollTimeMillisProp = "log.roll.ms";
  public static final String LogRollTimeHoursProp = "log.roll.hours";

  public static final String LogRollTimeJitterMillisProp = "log.roll.jitter.ms";
  public static final String LogRollTimeJitterHoursProp = "log.roll.jitter.hours";

  public static final String LogRetentionTimeMillisProp = "log.retention.ms";
  public static final String LogRetentionTimeMinutesProp = "log.retention.minutes";
  public static final String LogRetentionTimeHoursProp = "log.retention.hours";

  public static final String LogRetentionBytesProp = "log.retention.bytes";
  public static final String LogCleanupIntervalMsProp = "log.retention.check.interval.ms";
  public static final String LogCleanupPolicyProp = "log.cleanup.policy";
  public static final String LogCleanerThreadsProp = "log.cleaner.threads";
  public static final String LogCleanerIoMaxBytesPerSecondProp = "log.cleaner.io.max.bytes.per.second";
  public static final String LogCleanerDedupeBufferSizeProp = "log.cleaner.dedupe.buffer.size";
  public static final String LogCleanerIoBufferSizeProp = "log.cleaner.io.buffer.size";
  public static final String LogCleanerDedupeBufferLoadFactorProp = "log.cleaner.io.buffer.load.factor";
  public static final String LogCleanerBackoffMsProp = "log.cleaner.backoff.ms";
  public static final String LogCleanerMinCleanRatioProp = "log.cleaner.min.cleanable.ratio";
  public static final String LogCleanerEnableProp = "log.cleaner.enable";
  public static final String LogCleanerDeleteRetentionMsProp = "log.cleaner.delete.retention.ms";
  public static final String LogIndexSizeMaxBytesProp = "log.index.size.max.bytes";
  public static final String LogIndexIntervalBytesProp = "log.index.interval.bytes";
  public static final String LogFlushIntervalMessagesProp = "log.flush.interval.messages";
  public static final String LogDeleteDelayMsProp = "log.segment.delete.delay.ms";
  public static final String LogFlushSchedulerIntervalMsProp = "log.flush.scheduler.interval.ms";
  public static final String LogFlushIntervalMsProp = "log.flush.interval.ms";
  public static final String LogFlushOffsetCheckpointIntervalMsProp = "log.flush.offset.checkpoint.interval.ms";
  public static final String LogPreAllocateProp = "log.preallocate";
  
  /** ********* Kafka Metrics Configuration ***********/
  public static final String MetricNumSamplesProp = CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG;
  public static final String MetricSampleWindowMsProp = CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG;
  
  /** ********* Metric Configuration **************/
  public final int metricNumSamples = 2;
//  getInt(KafkaConfig.MetricNumSamplesProp) == null ? 2 : getInt(KafkaConfig.MetricNumSamplesProp);
  public final long metricSampleWindowMs = 3000; 
		  //getLong(KafkaConfig.MetricSampleWindowMsProp) == null ? 30000 : getLong(KafkaConfig.MetricSampleWindowMsProp);
//  public static final String LogMessageFormatVersionProp = LogConfigPrefix + LogConfig.MessageFormatVersionProp;
//  public static final String LogMessageTimestampTypeProp = LogConfigPrefix + LogConfig.MessageTimestampTypeProp;
//  public static final String LogMessageTimestampDifferenceMaxMsProp = LogConfigPrefix + LogConfig.MessageTimestampDifferenceMaxMsProp;
	  
	
  
  /** ********* Zookeeper Configuration ***********/
  public final String zkConnect = getString(KafkaConfig.ZkConnectProp);
  public final int zkSessionTimeoutMs = getInt(KafkaConfig.ZkSessionTimeoutMsProp);
//  public static final int zkConnectionTimeoutMs = Option(getInt(KafkaConfig.ZkConnectionTimeoutMsProp)).map(_.toInt).getOrElse(getInt(KafkaConfig.ZkSessionTimeoutMsProp));
  public final int zkSyncTimeMs = getInt(KafkaConfig.ZkSyncTimeMsProp);
  public final boolean ZkEnableSecureAcls = getBoolean(KafkaConfig.ZkEnableSecureAclsProp);
  public int brokerId = 0;
  public int SocketSendBufferBytes = 100 * 1024;
  public int SocketReceiveBufferBytes = 100 * 1024;
  public int SocketRequestMaxBytes = 100 * 1024 * 1024;
  public Map<SecurityProtocol, EndPoint> listeners = getListeners();
  
  public int NumNetworkThreads = Defaults.NumNetworkThreads;
  public int queuedMaxRequests = Defaults.QueuedMaxRequests;
  public int maxConnectionsPerIp = Defaults.MaxConnectionsPerIp;
  
  public Long connectionsMaxIdleMs = 10 * 60 * 1000L;
//  public int maxConnectionsPerIpOverrides = Defaults.maxConnectionsPerIpOverrides;
//  public final List<MetricsReporter> metricReporterClasses = getConfiguredInstances("metric.reporters", MetricsReporter.class);

	public static KafkaConfig fromProps(Properties props){
		return new KafkaConfig(propertiesToMap(props));
	}
	
	public static Map propertiesToMap(Properties props){
		return new HashMap<String, String>((Map) props);  
	}
	
	private Map<SecurityProtocol, EndPoint> getListeners(){
		Map<SecurityProtocol, EndPoint> map = new HashMap<SecurityProtocol, EndPoint>();
		map.put(SecurityProtocol.PLAINTEXT, new EndPoint("localhost", 9092, SecurityProtocol.PLAINTEXT));
		return map;
//		if(getString(KafkaConfig.ListenersProp) != null){
//			
//		}
	}
}

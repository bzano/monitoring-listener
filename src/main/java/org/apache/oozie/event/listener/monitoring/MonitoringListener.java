package org.apache.oozie.event.listener.monitoring;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.oozie.client.event.JobEvent;
import org.apache.oozie.event.BundleJobEvent;
import org.apache.oozie.event.CoordinatorActionEvent;
import org.apache.oozie.event.CoordinatorJobEvent;
import org.apache.oozie.event.WorkflowActionEvent;
import org.apache.oozie.event.WorkflowJobEvent;
import org.apache.oozie.event.listener.JobEventListener;
import org.apache.oozie.event.listener.monitoring.json.DateTypeAdapter;
import org.apache.oozie.util.XLog;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.gson.Gson;

public class MonitoringListener extends JobEventListener {
	private static final XLog LOGGER = XLog.getLog(MonitoringListener.class);
	private static final String SINGLE_PRODUCER = "producer";
	private static final String KAFK_BOOTSTRAP_SERVERS = "oozie.job.listener.kafka.bootstrap.servers";
	private static final String KAFKA_TOPIC = "oozie.job.listener.kafka.topic";

	private final Cache<String, KafkaProducer<String, String>> cache = CacheBuilder.newBuilder()
			.expireAfterAccess(5, TimeUnit.MINUTES)
			.removalListener((RemovalListener<String, KafkaProducer<String, String>>) notification -> {
				notification.getValue().close();
				LOGGER.info("Close unused producer");
			}).build();
	
	private Gson mapper = new Gson().newBuilder().setPrettyPrinting()
			.registerTypeAdapter(Date.class, new DateTypeAdapter()).create();

	private String kafkaBootstrapServers;
	private String kafkaTopic;

	public KafkaProducer<String, String> getProducer() {
		if (StringUtils.isEmpty(kafkaBootstrapServers) || StringUtils.isEmpty(kafkaTopic)) {
			LOGGER.error(KAFK_BOOTSTRAP_SERVERS + " / " + KAFKA_TOPIC + " are empty");
			return null;
		}
		KafkaProducer<String, String> kafkaProducer = cache.getIfPresent(SINGLE_PRODUCER);
		if (kafkaProducer == null) {
			Properties properties = new Properties();
			properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
			properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
			properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
			properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000);
			kafkaProducer = new KafkaProducer<>(properties);
			cache.put(SINGLE_PRODUCER, kafkaProducer);
		}
		return kafkaProducer;
	}

	public void closeProducer() {
		cache.invalidateAll();
		LOGGER.warn("Producer closed");
	}

	@Override
	public void init(Configuration conf) {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Monitoring listener initialized");
		}
		kafkaBootstrapServers = conf.get(KAFK_BOOTSTRAP_SERVERS);
		kafkaTopic = conf.get(KAFKA_TOPIC);
	}

	@Override
	public void destroy() {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Monitoring listener destroyed");
		}
		closeProducer();
	}

	@Override
	public void onWorkflowJobEvent(WorkflowJobEvent wje) {
		sendEventToKafka(eventToJson(wje));
	}

	@Override
	public void onWorkflowActionEvent(WorkflowActionEvent wae) {
		sendEventToKafka(eventToJson(wae));
	}

	@Override
	public void onCoordinatorJobEvent(CoordinatorJobEvent cje) {
		sendEventToKafka(eventToJson(cje));
	}

	@Override
	public void onCoordinatorActionEvent(CoordinatorActionEvent cae) {
		sendEventToKafka(eventToJson(cae));
	}

	@Override
	public void onBundleJobEvent(BundleJobEvent bje) {
		sendEventToKafka(eventToJson(bje));
	}

	private synchronized void sendEventToKafka(String event) {
		KafkaProducer<String, String> producer = null;
		try {
			producer = getProducer();
			if (producer != null) {
				Future<RecordMetadata> futureSend = producer
						.send(new ProducerRecord<String, String>(kafkaTopic, event));
				RecordMetadata metadata = futureSend.get();
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("[" + metadata + "] is sent");
				}
			}
		} catch (Exception th) {
			LOGGER.error("Error sending event (" + event + ") to kafka", th);
			closeProducer();
		}
	}

	private String eventToJson(JobEvent jobEvent) {
		return mapper.toJson(jobEvent);
	}
}

package com.zendesk.maxwell.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaProducerFactory {
	private static final Map<Properties, KafkaProducer<String, String>> store = new HashMap<>();
	public static KafkaProducer<String, String> get(Properties kafkaProperties) {
		synchronized (store) {
			return store.computeIfAbsent(kafkaProperties,
					properties -> new KafkaProducer<>(properties, new StringSerializer(), new StringSerializer()));
		}
	}
}

package ru.practicum.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import ru.practicum.kafka.dto.JokeDto;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class KafkaConsumerApplication {
	private static final Integer CONSUME_ATTEMPT_TIMEOUT_SECONDS = 4;
	private static final String CLIENT_ID = "simple-joke-consumer";
	private static final String GROUP_ID = "simple-terminal";
	private static final String SERVER = "localhost:9092";
	private static final String TOPIC = "jokes";
	private static final String KEY_DESERIALIZER = StringDeserializer.class.getCanonicalName();
	private static final String VALUE_DESERIALIZER = JokesAvroDeserializer.class.getCanonicalName();

	public static void main(String[] args) {
		Properties config = getConsumerProperties();

		try (KafkaConsumer<String, JokeDto> consumer = new KafkaConsumer<>(config)) {
			consumer.subscribe(List.of(TOPIC));
			ConsumerRecords<String, JokeDto> jokes = consumer.poll(Duration.ofSeconds(CONSUME_ATTEMPT_TIMEOUT_SECONDS));
			while (jokes.isEmpty()) {
				jokes = consumer.poll(Duration.ofSeconds(CONSUME_ATTEMPT_TIMEOUT_SECONDS));
			}
			jokes.forEach(record -> {
				var partition = record.partition();
				var offset = record.offset();
				var joke = record.value();
				System.out.printf("Consuming partition = %s, offset = %d, joke = %s%n", partition, offset, joke);
			});
			consumer.commitSync();
        }
    }

	private static Properties getConsumerProperties() {
		Properties properties = new Properties();
		properties.put("client.id", CLIENT_ID);
		properties.put("group.id", GROUP_ID);
		properties.put("bootstrap.servers", SERVER);
		properties.put("key.deserializer", KEY_DESERIALIZER);
		properties.put("value.deserializer", VALUE_DESERIALIZER);
		return properties;
	}
}

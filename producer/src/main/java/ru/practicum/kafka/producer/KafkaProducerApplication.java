package ru.practicum.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import ru.practicum.kafka.dto.JokeDto;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaProducerApplication {
    private static final String CLIENT_ID = "simple-joke-producer";
    private static final String SERVER = "localhost:9092";
    private static final String TOPIC = "jokes";
    private static final String KEY_SERIALIZER = StringSerializer.class.getCanonicalName();
    private static final String VALUE_SERIALIZER = AvroSerializer.class.getCanonicalName();

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties config = getProducerProperties();

        try (KafkaProducer<String, JokeDto> producer = new KafkaProducer<>(config)) {
            var joke = new Joke(-1, "Dark", "What do Japanese cannibals eat?", "Raw men");
            var jokeDto = toDto(joke);
            System.out.println("Producing joke = " + jokeDto);
            var record = new ProducerRecord<>(TOPIC, (String) null, jokeDto);
            Future<RecordMetadata> future = producer.send(record);
            future.get();
        }
    }

    private static Properties getProducerProperties() {
        Properties properties = new Properties();
        properties.put("client.id", CLIENT_ID);
        properties.put("bootstrap.servers", SERVER);
		properties.put("key.serializer", KEY_SERIALIZER);
        properties.put("value.serializer", VALUE_SERIALIZER);
        return properties;
    }

    private static JokeDto toDto(Joke joke) {
        return JokeDto.newBuilder()
                .setId(joke.id())
                .setCategory(joke.category())
                .setSetup(joke.setup())
                .setDelivery(joke.delivery())
                .build();
    }
}

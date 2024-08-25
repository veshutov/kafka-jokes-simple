package ru.practicum.kafka.consumer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import ru.practicum.kafka.dto.JokeDto;

import java.lang.reflect.InvocationTargetException;

public class JokesAvroDeserializer implements Deserializer<JokeDto> {
    private final Schema schema;

    public JokesAvroDeserializer()
            throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        this.schema = JokeDto.class.getDeclaredConstructor().newInstance().getSchema();
    }

    @Override
    public JokeDto deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            DatumReader<GenericRecord> datumReader = new SpecificDatumReader<>(schema);
            Decoder decoder = DecoderFactory.get().binaryDecoder(data, null);
            return (JokeDto) datumReader.read(null, decoder);
        } catch (Exception ex) {
            throw new SerializationException(ex);
        }
    }
}

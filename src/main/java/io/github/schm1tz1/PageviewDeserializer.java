package io.github.schm1tz1;

import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;

public class PageviewDeserializer extends KafkaJsonSchemaDeserializer<PageviewValue> {
}

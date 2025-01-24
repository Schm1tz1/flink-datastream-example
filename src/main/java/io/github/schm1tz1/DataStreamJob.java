/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.schm1tz1;

import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the tutorials and examples on the <a
 * href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run 'mvn clean package' on the
 * command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class DataStreamJob {

    private static final Logger logger = LoggerFactory.getLogger(DataStreamJob.class);

    private static Map<String, String> createSerdeConfig(Properties props, Class dataClass) {
        Map<String, String> config = new HashMap<>();
        props.forEach((key, value) -> config.put(key.toString(), value.toString()));
        config.put("json.value.type", dataClass.getName());
        return config;
    }

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        var kafkaSourceProps = FlinkConfigTools.readPropertiesFiles("/Users/rschmitz/workspace/clients/ccloud-basic.properties");
        var serDeConfig = createSerdeConfig(kafkaSourceProps, PageviewValue.class);

        var typeInfo = TypeInformation.of(PageviewValue.class);
        logger.info(typeInfo.toString());

        KafkaSource<PageviewValue> source =
                KafkaSource.<PageviewValue>builder()
                        .setProperties(kafkaSourceProps)
                        .setTopics("pageviews")
                        .setGroupId("flink-group")
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setDeserializer(
                            KafkaRecordDeserializationSchema.valueOnly(PageviewDeserializer.class, serDeConfig)
                        )
                        .build();

        DataStreamSource<PageviewValue> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        stream
            .print();

        // Execute program, beginning computation.
        env.execute("Flink Test Application");
    }
}

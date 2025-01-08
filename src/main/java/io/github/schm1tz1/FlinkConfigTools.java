package io.github.schm1tz1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Tools for Flink Pipeline configuration and initialization
 */
public class FlinkConfigTools {

    /**
     * The default logger.
     */
    static final Logger logger = LoggerFactory.getLogger(FlinkConfigTools.class);

    /**
     * Configure streams application using defaults and a configuration file.
     *
     * @param configFiles Additional properties to read from input files
     * @return configuration
     */
    public static Properties readStreamsConfigs(String... configFiles) {
        var properties = setDefaultStreamProperties();

        for (var configFile : configFiles) {
            FlinkConfigTools.readPropertiesFile(properties, configFile);
        }
        return properties;
    }

    /**
     * Configure application using no defaults and a configuration file.
     *
     * @param configFiles Additional properties to read from input files
     * @return Java Properties
     */
    public static Properties readPropertiesFiles(String... configFiles) {
        var properties = new Properties();

        for (var configFile : configFiles) {
            FlinkConfigTools.readPropertiesFile(properties, configFile);
        }

        return properties;
    }

    /**
     * Sets the default properties including bootstrap servers set to localhost. No pipeline
     * configuration included
     *
     * @return Properties object to start the pipeline
     */
    public static Properties setDefaultStreamProperties() {
        var properties = new Properties();
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "flink-consumer");
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        logger.info("Default streams properties:" + properties);

        return properties;
    }

    /**
     * Fills properties object with configuration read from file input
     *
     * @param properties Properties object to modify
     * @param configFile configuration file to read
     */
    public static void readPropertiesFile(Properties properties, String configFile) {
        if (configFile != null) {
            logger.info("Reading properties file " + configFile);

            try (InputStream inputStream = new FileInputStream(configFile)) {
                Reader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
                properties.load(reader);
                reader.close();

                logger.trace(
                        properties.entrySet().stream()
                                .map(e -> e.getKey() + " : " + e.getValue())
                                .collect(Collectors.joining(", ")));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                logger.error("Input properties file " + configFile + " not found");
                System.exit(1);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            logger.warn("No properties/config file specified, using defaults!");
        }
    }

    /**
     * Gets resource as stream.
     *
     * @param resourceFileName the resource file name
     * @return the resource as stream
     */
    @Nullable
    public static InputStream getResourceAsStream(String resourceFileName) {
        return Thread.currentThread().getContextClassLoader().getResourceAsStream(resourceFileName);
    }

    /**
     * Gets resource path.
     *
     * @param resource the resource
     * @return the resource path
     */
    public static String getResourcePath(String resource) {
        return Objects.requireNonNull(
                        Thread.currentThread().getContextClassLoader().getResource(resource))
                .getPath();
    }

    /**
     * Strips prefixes from a config map and passes through the other elements. Prefixed properties
     * can override non-prefixed.
     *
     * @param <V>    the type parameter
     * @param map    the input configuration map
     * @param prefix the prefix to be stripped
     * @return the map
     */
    public static <V> Map<String, V> stripPrefixes(Map<String, V> map, String prefix) {
        Map<String, V> result = new HashMap<>();
        for (Map.Entry<String, V> entry : map.entrySet()) {
            if (entry.getKey().startsWith(prefix) && entry.getKey().length() > prefix.length()) {
                result.put(entry.getKey().substring(prefix.length()), entry.getValue());
            } else {
                if (!result.containsKey(entry.getKey())) {
                    result.put(entry.getKey(), entry.getValue());
                }
            }
        }
        return result;
    }
}

import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;

public class TweetsHastagsCounter {

    public static ObjectMapper objectMapper = new ObjectMapper();
    public final static String TOPIC_NAME = "rawtweets";

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "KafkaTwitterLunia");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        final KStream<String, String> tweets = streamsBuilder.stream(TOPIC_NAME);

        tweets.flatMapValues(value -> Collections.singletonList(getHashtags(value)))
                .groupBy((key, value) -> value)
                .count()
                .toStream()
                .print(Printed.toSysOut());
    }

    public static String getHashtags(String input) {
        JsonNode root;
        try {
            root = objectMapper.readTree(input);
            JsonNode hashtagNode = root.path("entities").path("hashtags");
            if (!hashtagNode.toString().equals("[]")) {
                return hashtagNode.get(0).path("text").asText();
            }
        } catch (Exception exception) {
            exception.printStackTrace();
        }
        return "";
    }
}

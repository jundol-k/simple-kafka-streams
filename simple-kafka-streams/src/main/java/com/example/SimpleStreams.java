package com.example;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class SimpleStreams {
    private final static Logger logger = LoggerFactory.getLogger(SimpleStreams.class);
    private static String APPLICATION_NAME = "streams-application";
    private static String BOOTSTRAP_SERVER = "hLinux:9092";
    private static String STREAM_LOG = "stream_log";
    //private static String STREAM_LOG_COPY = "stream_log_copy";
    private static String STREAM_LOG_FILTER = "stream_log_filter";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder(); // 토폴로지 정의를 위한 용도
        KStream<String, String> streamLog = builder.stream(STREAM_LOG); // KStream 객체를 만든다. (소스 프로세서)

        // 매번 인스턴스를 만드는 스타일
        // KStream<String, String> filteredStream = streamLog.filter(((key, value) -> value.length() > 5));
        // filteredStream.to(STREAM_LOG_FILTER);

        // 플루언트 인터페이스 스타일 (매번 인스턴스를 생성하지 않음)
        streamLog.filter((key, value) -> value.length() > 5).to(STREAM_LOG_FILTER);

        //streamLog.to(STREAM_LOG_COPY); // 싱크 프로세서인 to() 메서드를 이용한다.

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }
}

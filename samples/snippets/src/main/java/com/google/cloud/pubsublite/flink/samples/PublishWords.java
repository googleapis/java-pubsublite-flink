package com.google.cloud.pubsublite.flink.samples;

import com.google.cloud.pubsublite.Message;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.flink.PubsubLiteSink;
import com.google.cloud.pubsublite.flink.PubsubLiteSinkSettings;
import com.google.cloud.pubsublite.proto.PubSubMessage;
import com.google.common.collect.ImmutableList;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PublishWords {

  public static void main(String[] args) throws Exception {
    PubsubLiteSinkSettings<Message> settings =
        PubsubLiteSinkSettings.messagesBuilder()
            .setTopicPath(TopicPath.parse("yolo"))
            .setMaxBytesOutstanding(1000)
            .build();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.fromCollection(ImmutableList.of("a", "b"))
        .map(s -> Message.fromProto(PubSubMessage.newBuilder().build()))
        .addSink(new PubsubLiteSink<>(settings));
    env.execute();
  }
}

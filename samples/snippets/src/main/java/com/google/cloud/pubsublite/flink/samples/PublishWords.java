package com.google.cloud.pubsublite.flink.samples;

import com.google.cloud.pubsublite.Message;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.flink.PubsubLiteSink;
import com.google.cloud.pubsublite.flink.PubsubLiteSinkSettings;
import com.google.cloud.pubsublite.proto.PubSubMessage;
import com.google.common.io.Resources;
import com.google.protobuf.ByteString;
import java.nio.charset.Charset;
import java.util.Arrays;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PublishWords {

  public static void main(String[] args) throws Exception {
    ParameterTool parameter = ParameterTool.fromArgs(args);
    // Read the resource, split into lines.
    String snippets =
        Resources.toString(Resources.getResource("words.txt"), Charset.defaultCharset());
    PubsubLiteSinkSettings<Message> settings =
        PubsubLiteSinkSettings.messagesBuilder()
            .setTopicPath(TopicPath.parse(parameter.get("topic")))
            .setMaxBytesOutstanding(1000)
            .build();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.fromCollection(Arrays.asList(snippets.split("\n")))
        .map(
            s ->
                Message.fromProto(
                    PubSubMessage.newBuilder().setData(ByteString.copyFromUtf8(s)).build()))
        .addSink(new PubsubLiteSink<>(settings));
    env.execute();
  }
}

package com.google.cloud.pubsublite.flink.samples;

import com.google.cloud.pubsublite.Message;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.flink.PubsubLiteSink;
import com.google.cloud.pubsublite.flink.PubsubLiteSinkSettings;
import com.google.cloud.pubsublite.proto.PubSubMessage;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import com.google.protobuf.ByteString;
import java.nio.charset.Charset;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class PublishWords {

  public static void main(String[] args) throws Exception {
    ParameterTool parameter = ParameterTool.fromArgs(args);
    // Read the resource, split into lines.
    String snippets =
        Resources.toString(Resources.getResource("words.txt"), Charset.defaultCharset());
    PubsubLiteSinkSettings<Message> settings = PubsubLiteSinkSettings
        .messagesBuilder()
        .setTopicPath(TopicPath.parse(parameter.get("topic")))
        .setMaxBytesOutstanding(1000)
        .build();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.fromCollection(ImmutableList.of(snippets)).flatMap(new LineSplitter()).map(
        s -> Message.fromProto(PubSubMessage.newBuilder().setData(ByteString.copyFromUtf8(s)).build())
    ).addSink(new PubsubLiteSink<>(settings));
    env.execute();
  }


  public static class LineSplitter implements FlatMapFunction<String, String> {
    @Override
    public void flatMap(String line, Collector<String> out) {
      line = line
          .replaceAll("[:;,.!]", "")
          .replaceAll("\n", " ")
          .replaceAll("\\s+", " ")
          .toLowerCase();
      for (String word : line.split(" ")) {
        out.collect(word);
      }
    }
  }
}

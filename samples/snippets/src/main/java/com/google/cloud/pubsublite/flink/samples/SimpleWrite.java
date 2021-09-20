package java.pubsublite.flink;

import com.google.cloud.pubsublite.Message;
import com.google.cloud.pubsublite.SequencedMessage;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.cloudpubsub.FlowControlSettings;
import com.google.cloud.pubsublite.flink.PubsubLiteSink;
import com.google.cloud.pubsublite.flink.PubsubLiteSinkSettings;
import com.google.cloud.pubsublite.flink.PubsubLiteSource;
import com.google.cloud.pubsublite.flink.PubsubLiteSourceSettings;
import com.google.cloud.pubsublite.proto.PubSubMessage;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class SimpleWrite {

    public static void main(String[] args) throws Exception {
      PubsubLiteSinkSettings<Message> settings = PubsubLiteSinkSettings
          .messagesBuilder()
          .setTopicPath(TopicPath.parse("yolo"))
          .setMaxBytesOutstanding(1000)
          .build();

      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.fromCollection(ImmutableList.of("a", "b")).map(
          s -> {
            return Message.fromProto(PubSubMessage.newBuilder().build());
          }
      ).addSink(
          new PubsubLiteSink<Message>(settings));
      env.execute();
    }
  }

package java.pubsublite.flink;

import com.google.cloud.pubsublite.SequencedMessage;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.cloudpubsub.FlowControlSettings;
import com.google.cloud.pubsublite.flink.PubsubLiteSource;
import com.google.cloud.pubsublite.flink.PubsubLiteSourceSettings;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SimpleRead {

  public static void main(String[] args) throws Exception {
    PubsubLiteSourceSettings<SequencedMessage> settings = PubsubLiteSourceSettings
        .messagesBuilder()
        .setFlowControlSettings(FlowControlSettings.builder()
            .setBytesOutstanding(1000L)
            .setMessagesOutstanding(1000L).build())
        .setSubscriptionPath(SubscriptionPath.parse("yolo"))
        .build();


    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.fromSource(
        new PubsubLiteSource<>(settings), WatermarkStrategy.noWatermarks(), "Source");
    env.execute();
  }
}

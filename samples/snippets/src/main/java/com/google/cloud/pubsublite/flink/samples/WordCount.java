package com.google.cloud.pubsublite.flink.samples;

import com.google.cloud.pubsublite.SequencedMessage;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.cloudpubsub.FlowControlSettings;
import com.google.cloud.pubsublite.flink.PubsubLiteSource;
import com.google.cloud.pubsublite.flink.PubsubLiteSourceSettings;
import com.google.cloud.pubsublite.flink.StopCondition;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;

public class WordCount {

  public static void main(String[] args) throws Exception {
    ParameterTool parameter = ParameterTool.fromArgs(args);

    PubsubLiteSourceSettings<SequencedMessage> settings = PubsubLiteSourceSettings
        .messagesBuilder()
        .setFlowControlSettings(FlowControlSettings.builder()
            .setBytesOutstanding(1000L)
            .setMessagesOutstanding(1000L).build())
        .setSubscriptionPath(SubscriptionPath.parse(parameter.get("subscription")))
        .setBoundedness(Boundedness.BOUNDED)
        .setStopCondition(StopCondition.readToHead())
        .build();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.fromSource(
        new PubsubLiteSource<>(settings), WatermarkStrategy.noWatermarks(), "Source")
        .addSink(new PrintSinkFunction<>());
    env.execute();
  }
  }


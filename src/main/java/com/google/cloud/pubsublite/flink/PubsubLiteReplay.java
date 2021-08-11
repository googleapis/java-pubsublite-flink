/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.pubsublite.flink;

import com.google.cloud.pubsublite.CloudZone;
import com.google.cloud.pubsublite.ProjectNumber;
import com.google.cloud.pubsublite.SubscriptionName;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.TopicName;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.cloudpubsub.FlowControlSettings;
import com.google.cloud.pubsublite.flink.internal.sink.PerServerPublisherCache;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction.Context;

class Configuration {

  private static final String ZONE = "us-central1-b";
  private static final String SUBSCRIPTION = "flink-dataproc-in";
  private static final String TOPIC = "flink-dataproc-out";

  private static ProjectNumber projectId() {
    return ProjectNumber.of(856003786687L);
  }

  private static TopicPath topicPath() {
    return TopicPath.newBuilder()
        .setLocation(CloudZone.parse(ZONE))
        .setProject(projectId())
        .setName(TopicName.of(TOPIC))
        .build();
  }

  private static SubscriptionPath subscriptionPath() {
    return SubscriptionPath.newBuilder()
        .setLocation(CloudZone.parse(ZONE))
        .setProject(projectId())
        .setName(SubscriptionName.of(SUBSCRIPTION))
        .build();
  }

  public static PubsubLiteSourceSettings.Builder<String> sourceSettings() {
    return PubsubLiteSourceSettings.builder(
            PubsubLiteDeserializationSchema.dataOnly(new SimpleStringSchema()))
        .setSubscriptionPath(subscriptionPath())
        .setFlowControlSettings(
            FlowControlSettings.builder()
                .setBytesOutstanding(10000)
                .setMessagesOutstanding(100)
                .build());
  }

  public static PubsubLiteSinkSettings.Builder<String> sinkSettings() {
    return PubsubLiteSinkSettings.builder(
            PubsubLiteSerializationSchema.dataOnly(new SimpleStringSchema()))
        .setTopicPath(topicPath());
  }
};

public class PubsubLiteReplay {

  // *************************************************************************
  // PROGRAM
  // *************************************************************************

  public static void main(String[] args) throws Exception {

    PubsubLiteSink<String> sink = new PubsubLiteSink<>(Configuration.sinkSettings().build());
    sink.open(new org.apache.flink.configuration.Configuration());
    sink.invoke("yolo", new Context() {
      @Override
      public long currentProcessingTime() {
        return 0;
      }

      @Override
      public long currentWatermark() {
        return 0;
      }

      @Override
      public Long timestamp() {
        return null;
      }
    });
    sink.close();
    System.out.println("published");

    // Checking input parameters
    final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);

    // set up the execution environment
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // make parameters available in the web interface
    env.getConfig().setGlobalJobParameters(params);

    System.err.println("Configuring pipeline");
    env.fromSource(
            new PubsubLiteSource<>(Configuration.sourceSettings().build()),
            WatermarkStrategy.noWatermarks(),
            "testPSL")
        .addSink(new PubsubLiteSink<>(Configuration.sinkSettings().build()));
    System.err.println("Running pipeline");

    // execute program
    env.execute("Pubsub Lite Replay");
  }
}

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

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.spy;

import com.google.api.core.ApiFutures;
import com.google.cloud.pubsublite.AdminClient;
import com.google.cloud.pubsublite.CloudZone;
import com.google.cloud.pubsublite.Message;
import com.google.cloud.pubsublite.MessageMetadata;
import com.google.cloud.pubsublite.ProjectId;
import com.google.cloud.pubsublite.SubscriptionName;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.TopicName;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.cloudpubsub.FlowControlSettings;
import com.google.cloud.pubsublite.flink.internal.sink.PerServerPublisherCache;
import com.google.cloud.pubsublite.internal.Publisher;
import com.google.cloud.pubsublite.proto.Subscription;
import com.google.cloud.pubsublite.proto.Subscription.DeliveryConfig;
import com.google.cloud.pubsublite.proto.Subscription.DeliveryConfig.DeliveryRequirement;
import com.google.cloud.pubsublite.proto.Topic;
import com.google.cloud.pubsublite.proto.Topic.PartitionConfig;
import com.google.cloud.pubsublite.proto.Topic.PartitionConfig.Capacity;
import com.google.cloud.pubsublite.proto.Topic.RetentionConfig;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mockito;

public class ITSourceAndSinkTest {
  // Callers must set GCLOUD_PROJECT
  private static final ProjectId PROJECT = ProjectId.of(System.getenv("GCLOUD_PROJECT"));
  private static final CloudZone ZONE = CloudZone.parse("us-central1-b");
  private static final List<String> INTEGER_STRINGS =
      IntStream.range(0, 100).mapToObj(Integer::toString).collect(Collectors.toList());
  private static final SimpleStringSchema SCHEMA = new SimpleStringSchema();

  TopicPath topicPath;
  SubscriptionPath subscriptionPath;

  @ClassRule
  public static MiniClusterWithClientResource flinkCluster =
      new MiniClusterWithClientResource(
          new MiniClusterResourceConfiguration.Builder()
              .setNumberSlotsPerTaskManager(2)
              .setNumberTaskManagers(1)
              .build());

  private PubsubLiteSourceSettings.Builder<String> sourceSettings() {
    return PubsubLiteSourceSettings.builder(
            PubsubLiteDeserializationSchema.dataOnly(new SimpleStringSchema()))
        .setSubscriptionPath(subscriptionPath)
        .setFlowControlSettings(
            FlowControlSettings.builder()
                .setBytesOutstanding(100000)
                .setMessagesOutstanding(100)
                .build());
  }

  private PubsubLiteSinkSettings.Builder<String> sinkSettings() {
    return PubsubLiteSinkSettings.builder(
            PubsubLiteSerializationSchema.dataOnly(new SimpleStringSchema()))
        .setTopicPath(topicPath);
  }

  private Publisher<MessageMetadata> getPublisher() {
    return PerServerPublisherCache.getOrCreate(sinkSettings().build().getPublisherConfig());
  }

  private AdminClient getAdminClient() {
    return sourceSettings().build().getAdminClient();
  }

  private static Message messageFromString(String i) {
    return Message.builder().setData(ByteString.copyFrom(SCHEMA.serialize(i))).build();
  }

  @Before
  public void createResources() throws Exception {
    UUID uuid = UUID.randomUUID();
    topicPath =
        TopicPath.newBuilder()
            .setLocation(ZONE)
            .setProject(PROJECT)
            .setName(TopicName.of("flink-integration-test-topic-" + uuid.toString()))
            .build();
    subscriptionPath =
        SubscriptionPath.newBuilder()
            .setLocation(ZONE)
            .setProject(PROJECT)
            .setName(SubscriptionName.of("flink-test-sub-" + uuid.toString()))
            .build();

    Topic topic =
        Topic.newBuilder()
            .setName(topicPath.toString())
            .setPartitionConfig(
                PartitionConfig.newBuilder()
                    .setCount(2)
                    .setCapacity(
                        Capacity.newBuilder()
                            .setPublishMibPerSec(4)
                            .setSubscribeMibPerSec(4)
                            .build())
                    .build())
            .setRetentionConfig(
                RetentionConfig.newBuilder().setPerPartitionBytes(32212254720L).build())
            .build();
    Subscription subscription =
        Subscription.newBuilder()
            .setTopic(topicPath.toString())
            .setDeliveryConfig(
                DeliveryConfig.newBuilder()
                    .setDeliveryRequirement(DeliveryRequirement.DELIVER_IMMEDIATELY)
                    .build())
            .setName(subscriptionPath.toString())
            .build();
    try (AdminClient client = getAdminClient()) {
      client.createTopic(topic).get();
      client.createSubscription(subscription).get();
    }
    CollectSink.clear();
    staticSet.clear();
    StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(2).enableCheckpointing(100);
  }

  @After
  public void destroyResources() throws Exception {
    try (AdminClient client = getAdminClient()) {
      client.deleteTopic(topicPath).get();
      client.deleteSubscription(subscriptionPath).get();
    }
    CollectSink.clear();
    staticSet.clear();
  }

  @Test
  public void testSource() throws Exception {
    Publisher<MessageMetadata> publisher = getPublisher();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.fromSource(
            new PubsubLiteSource<>(sourceSettings().build()),
            WatermarkStrategy.noWatermarks(),
            "testPSL")
        .addSink(new CollectSink());
    JobClient client = env.executeAsync();

    ApiFutures.allAsList(
            INTEGER_STRINGS.stream()
                .map(v -> publisher.publish(messageFromString(v)))
                .collect(Collectors.toList()))
        .get();

    while (CollectSink.values().size() < INTEGER_STRINGS.size()) {
      if (client.getJobExecutionResult().isCompletedExceptionally()) {
        client.getJobExecutionResult().get();
      }
      Thread.sleep(100);
    }
    assertThat(CollectSink.values()).containsExactlyElementsIn(INTEGER_STRINGS);
    client.cancel().get();
  }

  @Test
  public void testBoundedSource() throws Exception {
    Publisher<MessageMetadata> publisher = getPublisher();

    ApiFutures.allAsList(
            INTEGER_STRINGS.stream()
                .map(v -> publisher.publish(messageFromString(v)))
                .collect(Collectors.toList()))
        .get();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.fromSource(
            new PubsubLiteSource<>(
                sourceSettings()
                    .setBoundedness(Boundedness.BOUNDED)
                    .setStopCondition(StopCondition.readToHead())
                    .build()),
            WatermarkStrategy.noWatermarks(),
            "testPSL")
        .addSink(new CollectSink());
    env.execute();
    assertThat(CollectSink.values()).containsExactlyElementsIn(INTEGER_STRINGS);
  }

  @Test
  public void testSourceWithFailure() throws Exception {

    Publisher<MessageMetadata> publisher = getPublisher();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setRestartStrategy(
        RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.MILLISECONDS)));
    env.fromSource(
            new PubsubLiteSource<>(sourceSettings().build()),
            WatermarkStrategy.noWatermarks(),
            "testPSL")
        .filter(
            v -> {
              if (v.equals(INTEGER_STRINGS.get(37)) && staticSet.add("mapFailOnce")) {
                throw new RuntimeException("oh no, it's 37!");
              }
              return true;
            })
        .addSink(new CollectSink());
    JobClient client = env.executeAsync();

    ApiFutures.allAsList(
            INTEGER_STRINGS.stream()
                .map(v -> publisher.publish(messageFromString(v)))
                .collect(Collectors.toList()))
        .get();

    while (CollectSink.values().size() < INTEGER_STRINGS.size()) {
      if (client.getJobExecutionResult().isCompletedExceptionally()) {
        client.getJobExecutionResult().get();
      }
      Thread.sleep(100);
    }
    assertThat(CollectSink.values()).containsExactlyElementsIn(INTEGER_STRINGS);
    client.cancel().get();
  }

  @Test
  public void testSink() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.fromCollection(INTEGER_STRINGS)
        .addSink(new PubsubLiteSink<>(sinkSettings().build()))
        .name("PSL Sink");

    env.fromSource(
            new PubsubLiteSource<>(sourceSettings().build()),
            WatermarkStrategy.noWatermarks(),
            "testPSL")
        .addSink(new CollectSink());

    JobClient client = env.executeAsync();

    while (CollectSink.values().size() < INTEGER_STRINGS.size()) {
      if (client.getJobExecutionResult().isCompletedExceptionally()) {
        client.getJobExecutionResult().get();
      }
      Thread.sleep(100);
    }
    assertThat(CollectSink.values()).containsExactlyElementsIn(INTEGER_STRINGS);
    client.cancel().get();
  }

  @Test
  public void testSinkWithFailure() throws Exception {
    // Set up a publisher which will fail once when attempting to publish the 37th message
    Publisher<MessageMetadata> publisher =
        spy(PerServerPublisherCache.getOrCreate(sinkSettings().build().getPublisherConfig()));
    Mockito.doAnswer(
            inv -> {
              Message m = inv.getArgument(0);
              if (m.data().toStringUtf8().equals(INTEGER_STRINGS.get(37))
                  && staticSet.add("publishFailOnce")) {
                return ApiFutures.immediateFailedFuture(new RuntimeException("failure"));
              }
              return inv.callRealMethod();
            })
        .when(publisher)
        .publish(any());
    PerServerPublisherCache.getCache().set(sinkSettings().build().getPublisherConfig(), publisher);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setRestartStrategy(
        RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.MILLISECONDS)));

    env.fromCollection(INTEGER_STRINGS)
        .addSink(new PubsubLiteSink<>(sinkSettings().build()))
        .name("PSL Sink");

    env.fromSource(
            new PubsubLiteSource<>(sourceSettings().build()),
            WatermarkStrategy.noWatermarks(),
            "testPSL")
        .addSink(new CollectSink());

    JobClient client = env.executeAsync();

    while (CollectSink.values().size() < INTEGER_STRINGS.size()) {
      if (client.getJobExecutionResult().isCompletedExceptionally()) {
        client.getJobExecutionResult().get();
      }
      Thread.sleep(100);
    }
    assertThat(CollectSink.values()).containsExactlyElementsIn(INTEGER_STRINGS);
    client.cancel().get();
  }

  // A set of static strings for simulating persisted storage in pipelines.
  private static final Set<String> staticSet = Collections.synchronizedSet(new HashSet<>());

  // A testing sink which stores messages in a static map to prevent them from being lost when
  // the sink is serialized.
  private static class CollectSink implements SinkFunction<String>, Serializable {
    // Note: doesn't store duplicates.
    private static final Set<String> collector = Collections.synchronizedSet(new HashSet<>());

    @Override
    public void invoke(String value) throws Exception {
      collector.add(value);
    }

    public static Collection<String> values() {
      return ImmutableList.copyOf(collector);
    }

    public static void clear() {
      collector.clear();
    }
  }
}

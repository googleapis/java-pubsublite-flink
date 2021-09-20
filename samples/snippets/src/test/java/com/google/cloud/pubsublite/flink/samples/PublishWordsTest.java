package pubsublite.flink;

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

import static com.google.common.truth.Truth.assertThat;

import com.google.api.core.ApiFutures;
import com.google.cloud.pubsublite.AdminClient;
import com.google.cloud.pubsublite.AdminClientSettings;
import com.google.cloud.pubsublite.CloudZone;
import com.google.cloud.pubsublite.Message;
import com.google.cloud.pubsublite.MessageMetadata;
import com.google.cloud.pubsublite.ProjectId;
import com.google.cloud.pubsublite.SubscriptionName;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.TopicName;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.cloudpubsub.FlowControlSettings;
import com.google.cloud.pubsublite.flink.PartitionFinishedCondition;
import com.google.cloud.pubsublite.flink.PartitionFinishedCondition.Result;
import com.google.cloud.pubsublite.flink.PubsubLiteDeserializationSchema;
import com.google.cloud.pubsublite.flink.PubsubLiteSerializationSchema;
import com.google.cloud.pubsublite.flink.PubsubLiteSink;
import com.google.cloud.pubsublite.flink.PubsubLiteSinkSettings;
import com.google.cloud.pubsublite.flink.PubsubLiteSource;
import com.google.cloud.pubsublite.flink.PubsubLiteSourceSettings;
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
import java.pubsublite.flink.SimpleRead;
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

public class PublishWordsTest {
  // Callers must set GCLOUD_PROJECT
  private static final ProjectId PROJECT = ProjectId.of("palmeretest");
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


  private AdminClient getAdminClient() {
    return AdminClient.create(AdminClientSettings.newBuilder().setRegion(ZONE.region()).build());
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
    SimpleRead.main(new String[]{});
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


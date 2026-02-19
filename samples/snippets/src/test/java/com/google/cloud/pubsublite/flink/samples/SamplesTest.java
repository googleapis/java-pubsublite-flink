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

package com.google.cloud.pubsublite.flink.samples;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.pubsublite.AdminClient;
import com.google.cloud.pubsublite.AdminClientSettings;
import com.google.cloud.pubsublite.CloudZone;
import com.google.cloud.pubsublite.ProjectId;
import com.google.cloud.pubsublite.SubscriptionName;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.TopicName;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.proto.Subscription;
import com.google.cloud.pubsublite.proto.Subscription.DeliveryConfig;
import com.google.cloud.pubsublite.proto.Subscription.DeliveryConfig.DeliveryRequirement;
import com.google.cloud.pubsublite.proto.Topic;
import com.google.cloud.pubsublite.proto.Topic.PartitionConfig;
import com.google.cloud.pubsublite.proto.Topic.PartitionConfig.Capacity;
import com.google.cloud.pubsublite.proto.Topic.RetentionConfig;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class SamplesTest {
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
    StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(2).enableCheckpointing(100);
  }

  @After
  public void destroyResources() throws Exception {
    try (AdminClient client = getAdminClient()) {
      client.deleteTopic(topicPath).get();
      client.deleteSubscription(subscriptionPath).get();
    }
  }

  @Test
  public void testSimpleReadWrites() throws Exception {
    ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    PrintStream originalOut = System.out;
    try {
      System.setOut(new PrintStream(outContent));
      SimpleWrite.main(new String[] {"--topic", topicPath.toString()});
      SimpleRead.main(new String[] {"--subscription", subscriptionPath.toString()});
    } finally {
      System.setOut(originalOut);
    }
    String output = outContent.toString();
    for (int i = 0; i < 1000; i++) {
      assertThat(output).contains("message " + i);
    }
  }

  @Test
  public void testWordCount() throws Exception {
    ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    PrintStream originalOut = System.out;
    try {
      System.setOut(new PrintStream(outContent));
      PublishWords.main(new String[] {"--topic", topicPath.toString()});
      WordCount.main(new String[] {"--subscription", subscriptionPath.toString()});
    } finally {
      System.setOut(originalOut);
    }
    String output = outContent.toString();
    assertThat(output).contains("the,24");
  }
}

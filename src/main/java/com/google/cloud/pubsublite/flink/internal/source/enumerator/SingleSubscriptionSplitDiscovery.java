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
package com.google.cloud.pubsublite.flink.internal.source.enumerator;

import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsublite.AdminClient;
import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.flink.internal.source.split.SubscriptionPartitionSplit;
import com.google.cloud.pubsublite.flink.proto.SplitEnumeratorCheckpoint;
import com.google.cloud.pubsublite.internal.CursorClient;
import com.google.cloud.pubsublite.internal.ExtractStatus;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class SingleSubscriptionSplitDiscovery implements SplitDiscovery {
  private final AdminClient adminClient;
  private final CursorClient cursorClient;
  private final TopicPath topicPath;
  private final SubscriptionPath subscriptionPath;
  private long partitionCount;

  private SingleSubscriptionSplitDiscovery(
      AdminClient adminClient,
      CursorClient cursorClient,
      TopicPath topicPath,
      SubscriptionPath subscriptionPath,
      long partitionCount) {
    this.adminClient = adminClient;
    this.cursorClient = cursorClient;
    this.topicPath = topicPath;
    this.subscriptionPath = subscriptionPath;
    this.partitionCount = partitionCount;
  }

  public static SingleSubscriptionSplitDiscovery create(
      AdminClient adminClient,
      CursorClient cursorClient,
      TopicPath topicPath,
      SubscriptionPath subscriptionPath) {
    return new SingleSubscriptionSplitDiscovery(
        adminClient, cursorClient, topicPath, subscriptionPath, 0L);
  }

  public static SingleSubscriptionSplitDiscovery fromCheckpoint(
      SplitEnumeratorCheckpoint.Discovery proto,
      Collection<SubscriptionPartitionSplit> currentSplits,
      AdminClient adminClient,
      CursorClient cursorClient) {
    SubscriptionPath subscriptionPath = SubscriptionPath.parse(proto.getSubscription());
    TopicPath topicPath = TopicPath.parse(proto.getTopic());
    Set<Long> partitions = new TreeSet<>();
    for (SubscriptionPartitionSplit s : currentSplits) {
      if (!s.subscriptionPath().equals(subscriptionPath)) {
        throw new IllegalStateException(
            "Split discovery configured with subscription "
                + subscriptionPath
                + " but current splits contains a split from subscription "
                + s);
      }
      partitions.add(s.partition().value());
    }
    long partitionCount = partitions.size();
    for (long p = 0; p < partitions.size(); p++) {
      if (!partitions.contains(p)) {
        throw new IllegalStateException(
            "Split set is not continuous, missing split for partition " + p + " " + currentSplits);
      }
    }
    return new SingleSubscriptionSplitDiscovery(
        adminClient, cursorClient, topicPath, subscriptionPath, partitionCount);
  }

  public synchronized List<SubscriptionPartitionSplit> discoverNewSplits() throws ApiException {
    try {
      List<SubscriptionPartitionSplit> newSplits = new ArrayList<>();
      long newPartitionCount = adminClient.getTopicPartitionCount(topicPath).get();
      if (newPartitionCount == partitionCount) {
        return newSplits;
      }
      Map<Partition, Offset> cursorMap = cursorClient.listPartitionCursors(subscriptionPath).get();
      for (long p = partitionCount; p < newPartitionCount; p++) {
        Partition partition = Partition.of(p);
        Offset offset = cursorMap.getOrDefault(partition, Offset.of(0));
        newSplits.add(SubscriptionPartitionSplit.create(subscriptionPath, partition, offset));
      }
      partitionCount = newPartitionCount;
      return newSplits;
    } catch (Throwable t) {
      throw ExtractStatus.toCanonical(t).underlying;
    }
  }

  public synchronized SplitEnumeratorCheckpoint.Discovery checkpoint() {
    return SplitEnumeratorCheckpoint.Discovery.newBuilder()
        .setSubscription(subscriptionPath.toString())
        .setTopic(topicPath.toString())
        .build();
  }
}

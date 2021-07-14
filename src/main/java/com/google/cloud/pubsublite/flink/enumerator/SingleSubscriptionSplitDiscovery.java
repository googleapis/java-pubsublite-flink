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
package com.google.cloud.pubsublite.flink.enumerator;

import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsublite.*;
import com.google.cloud.pubsublite.flink.proto.SplitEnumeratorCheckpoint;
import com.google.cloud.pubsublite.flink.split.SubscriptionPartitionSplit;
import com.google.cloud.pubsublite.internal.CursorClient;
import com.google.cloud.pubsublite.internal.ExtractStatus;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

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

  static SingleSubscriptionSplitDiscovery create(
      AdminClient adminClient,
      CursorClient cursorClient,
      TopicPath topicPath,
      SubscriptionPath subscriptionPath) {
    return new SingleSubscriptionSplitDiscovery(
        adminClient, cursorClient, topicPath, subscriptionPath, 0L);
  }

  static SingleSubscriptionSplitDiscovery fromCheckpoint(
      SplitEnumeratorCheckpoint.Discovery proto,
      Collection<SubscriptionPartitionSplit> currentSplits,
      AdminClient adminClient,
      CursorClient cursorClient) {
    SubscriptionPath subscriptionPath = SubscriptionPath.parse(proto.getSubscription());
    TopicPath topicPath = TopicPath.parse(proto.getTopic());
    long partitionCount =
        currentSplits.stream()
            .filter(s -> s.subscriptionPath().equals(subscriptionPath))
            .mapToLong(a -> a.partition().value())
            .max()
            .orElse(0);
    return new SingleSubscriptionSplitDiscovery(
        adminClient, cursorClient, topicPath, subscriptionPath, partitionCount);
  }

  public List<SubscriptionPartitionSplit> discoverSplits() throws ApiException {
    List<SubscriptionPartitionSplit> newSplits = new ArrayList<>();
    long newPartitionCount;
    try {
      newPartitionCount = adminClient.getTopicPartitionCount(topicPath).get();
    } catch (ExecutionException | InterruptedException e) {
      throw ExtractStatus.toCanonical(e).underlying;
    }
    if (newPartitionCount == partitionCount) {
      return newSplits;
    }
    Map<Partition, Offset> cursorMap;
    try {
      cursorMap = cursorClient.listPartitionCursors(subscriptionPath).get();
    } catch (ExecutionException | InterruptedException e) {
      throw ExtractStatus.toCanonical(e).underlying;
    }
    for (long p = partitionCount; p < newPartitionCount; p++) {
      Partition partition = Partition.of(p);
      Offset offset = cursorMap.getOrDefault(partition, Offset.of(0));
      newSplits.add(SubscriptionPartitionSplit.create(subscriptionPath, partition, offset));
    }
    partitionCount = newPartitionCount;
    return newSplits;
  }

  public SplitEnumeratorCheckpoint.Discovery checkpoint() {
    return SplitEnumeratorCheckpoint.Discovery.newBuilder()
        .setSubscription(subscriptionPath.toString())
        .setTopic(topicPath.toString())
        .build();
  }

  @Override
  public void close() {
    try (AdminClient a = adminClient;
        CursorClient c = cursorClient) {}
  }
}

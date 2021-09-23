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

import com.google.api.core.ApiFutures;
import com.google.cloud.pubsublite.AdminClient;
import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.flink.internal.reader.PartitionFinishedCondition;
import com.google.cloud.pubsublite.flink.internal.reader.PartitionFinishedCondition.Factory;
import com.google.cloud.pubsublite.flink.internal.reader.PartitionFinishedCondition.Result;
import com.google.cloud.pubsublite.internal.ExtractStatus;
import com.google.cloud.pubsublite.internal.TopicStatsClient;
import com.google.cloud.pubsublite.proto.Cursor;
import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public abstract class StopCondition implements Serializable {
  // Called to convert conditions with placeholder values like "HEAD" to an offset based condition.
  abstract StopCondition canonicalize(
      SubscriptionPath path,
      Supplier<AdminClient> adminClient,
      Supplier<TopicStatsClient> topicStatsClient);

  abstract PartitionFinishedCondition.Factory toFinishCondition();

  private static class ContinueIndefinitely extends StopCondition {
    @Override
    public StopCondition canonicalize(
        SubscriptionPath path,
        Supplier<AdminClient> adminClient,
        Supplier<TopicStatsClient> topicStatsClient) {
      return this;
    }

    @Override
    public Factory toFinishCondition() {
      return (subscription, partition) -> offset -> Result.CONTINUE;
    }
  }

  private static class ReadToOffsets extends StopCondition {
    private final Map<Partition, Offset> offsets;

    private ReadToOffsets(Map<Partition, Offset> offsets) {
      this.offsets = offsets;
    }

    @Override
    public StopCondition canonicalize(
        SubscriptionPath path,
        Supplier<AdminClient> adminClient,
        Supplier<TopicStatsClient> topicStatsClient) {
      return this;
    }

    @Override
    public Factory toFinishCondition() {
      return (subscription, partition) ->
          offset -> {
            Offset stopOffset = offsets.getOrDefault(partition, Offset.of(0L));
            if (offset.value() >= stopOffset.value()) return Result.FINISH_BEFORE;
            if (offset.value() == stopOffset.value() - 1) return Result.FINISH_AFTER;
            return Result.CONTINUE;
          };
    }
  }

  private static class ReadToHead extends StopCondition {
    private static Map<Partition, Offset> getHeadOffsets(
        SubscriptionPath path, AdminClient admin, TopicStatsClient topicStats) {
      long partitionCount;
      List<Cursor> cursors;
      try {
        TopicPath topicPath = TopicPath.parse(admin.getSubscription(path).get().getTopic());
        partitionCount = admin.getTopicPartitionCount(topicPath).get();
        cursors =
            ApiFutures.allAsList(
                    LongStream.range(0, partitionCount)
                        .mapToObj(
                            partition ->
                                topicStats.computeHeadCursor(topicPath, Partition.of(partition)))
                        .collect(Collectors.toList()))
                .get();
      } catch (Throwable t) {
        throw ExtractStatus.toCanonical(t).underlying;
      }
      ImmutableMap.Builder<Partition, Offset> offsets = ImmutableMap.builder();
      for (int i = 0; i < partitionCount; i++) {
        offsets.put(Partition.of(i), Offset.of(cursors.get(i).getOffset()));
      }
      return offsets.build();
    }

    @Override
    public StopCondition canonicalize(
        SubscriptionPath path,
        Supplier<AdminClient> adminClient,
        Supplier<TopicStatsClient> topicStatsClient) {
      return readToOffsets(getHeadOffsets(path, adminClient.get(), topicStatsClient.get()));
    }

    @Override
    public Factory toFinishCondition() {
      throw new IllegalStateException("Cannot translate to stop condition before canonicalizing");
    }
  }

  // The flink source will continue reading messages indefinitely.
  public static StopCondition continueIndefinitely() {
    return new ContinueIndefinitely();
  }

  // The flink source will read to the specified offset for each partition. If no offset is supplied
  // for a partition, the source will not read any messages from that partition.
  public static StopCondition readToOffsets(Map<Partition, Offset> offsets) {
    return new ReadToOffsets(offsets);
  }

  // Read to the head offset for every partition. Head is evaluated when the source settings are
  // built.
  public static StopCondition readToHead() {
    return new ReadToHead();
  }
}

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

public class StopCondition implements Serializable {
  enum Condition {
    INDEFINITE,
    HEAD,
    OFFSETS,
  }

  private final Condition condition;
  private final Map<Partition, Offset> finalSplits;

  private StopCondition(Condition condition, Map<Partition, Offset> finalSplits) {
    this.condition = condition;
    this.finalSplits = finalSplits;
  }

  private Map<Partition, Offset> getHeadOffsets(
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

  // Called to convert conditions with placeholder values like "HEAD" to an offset based condition.
  StopCondition canonicalize(
      SubscriptionPath path,
      Supplier<AdminClient> adminClient,
      Supplier<TopicStatsClient> topicStatsClient) {
    switch (condition) {
      case INDEFINITE:
      case OFFSETS:
        return this;
      case HEAD:
        return readToOffsets(getHeadOffsets(path, adminClient.get(), topicStatsClient.get()));
      default:
        throw new IllegalStateException("illegal enum value " + condition);
    }
  }

  PartitionFinishedCondition.Factory toFinishCondition() {
    switch (condition) {
      case INDEFINITE:
        return (subscription, partition) -> offset -> Result.CONTINUE;
      case OFFSETS:
        return (subscription, partition) ->
            offset -> {
              Offset stopOffset = finalSplits.getOrDefault(partition, Offset.of(0L));
              if (offset.value() >= stopOffset.value()) return Result.FINISH_BEFORE;
              if (offset.value() == stopOffset.value() - 1) return Result.FINISH_AFTER;
              return Result.CONTINUE;
            };
      default:
        throw new IllegalStateException("illegal enum value " + condition);
    }
  }

  public static StopCondition continueIndefinitely() {
    return new StopCondition(Condition.INDEFINITE, ImmutableMap.of());
  }

  public static StopCondition readToHead() {
    return new StopCondition(Condition.HEAD, ImmutableMap.of());
  }

  public static StopCondition readToOffsets(Map<Partition, Offset> offsets) {
    return new StopCondition(Condition.OFFSETS, offsets);
  }
}

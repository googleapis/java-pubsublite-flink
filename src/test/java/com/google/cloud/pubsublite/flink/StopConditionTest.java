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

import static com.google.cloud.pubsublite.internal.testing.UnitTestExamples.exampleOffset;
import static com.google.cloud.pubsublite.internal.testing.UnitTestExamples.examplePartition;
import static com.google.cloud.pubsublite.internal.testing.UnitTestExamples.exampleSubscription;
import static com.google.cloud.pubsublite.internal.testing.UnitTestExamples.exampleSubscriptionPath;
import static com.google.cloud.pubsublite.internal.testing.UnitTestExamples.exampleTopicPath;
import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFutures;
import com.google.cloud.pubsublite.AdminClient;
import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.flink.internal.reader.PartitionFinishedCondition.Result;
import com.google.cloud.pubsublite.internal.TopicStatsClient;
import com.google.cloud.pubsublite.proto.Cursor;
import com.google.common.collect.ImmutableMap;
import java.util.function.Supplier;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class StopConditionTest {
  @Mock Supplier<AdminClient> adminClientSupplier;
  @Mock AdminClient adminClient;
  @Mock Supplier<TopicStatsClient> topicStatsClientSupplier;
  @Mock TopicStatsClient topicStatsClient;

  @Test
  public void testContinueIndefinitely() {
    StopCondition condition = StopCondition.continueIndefinitely();
    StopCondition canonical =
        condition.canonicalize(
            exampleSubscriptionPath(), adminClientSupplier, topicStatsClientSupplier);

    assertThat(
            canonical
                .toFinishCondition()
                .New(exampleSubscriptionPath(), examplePartition())
                .partitionFinished(exampleOffset()))
        .isEqualTo(Result.CONTINUE);
  }

  @Test
  public void testStopAtOffsets() {
    StopCondition condition =
        StopCondition.readToOffsets(ImmutableMap.of(examplePartition(), exampleOffset()));
    StopCondition canonical =
        condition.canonicalize(
            exampleSubscriptionPath(), adminClientSupplier, topicStatsClientSupplier);

    assertThat(
            canonical
                .toFinishCondition()
                .New(exampleSubscriptionPath(), examplePartition())
                .partitionFinished(exampleOffset()))
        .isEqualTo(Result.FINISH_BEFORE);
  }

  @Test
  public void testStopAtHead() {
    StopCondition condition = StopCondition.readToHead();

    when(adminClientSupplier.get()).thenReturn(adminClient);
    when(topicStatsClientSupplier.get()).thenReturn(topicStatsClient);

    when(adminClient.getSubscription(exampleSubscriptionPath()))
        .thenReturn(ApiFutures.immediateFuture(exampleSubscription()));
    when(adminClient.getTopicPartitionCount(exampleTopicPath()))
        .thenReturn(ApiFutures.immediateFuture(1L));
    when(topicStatsClient.computeHeadCursor(exampleTopicPath(), Partition.of(0)))
        .thenReturn(ApiFutures.immediateFuture(Cursor.newBuilder().setOffset(10).build()));

    StopCondition canonical =
        condition.canonicalize(
            exampleSubscriptionPath(), adminClientSupplier, topicStatsClientSupplier);

    assertThat(
            canonical
                .toFinishCondition()
                .New(exampleSubscriptionPath(), Partition.of(10))
                .partitionFinished(Offset.of(10)))
        .isEqualTo(Result.FINISH_BEFORE);
  }
}

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
package com.google.cloud.pubsublite.flink.reader;

import static com.google.cloud.pubsublite.flink.TestUtilities.messageFromOffset;
import static com.google.cloud.pubsublite.internal.testing.UnitTestExamples.examplePartition;
import static com.google.cloud.pubsublite.internal.testing.UnitTestExamples.exampleSubscriptionPath;
import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFutures;
import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.SequencedMessage;
import com.google.cloud.pubsublite.flink.PartitionFinishedCondition;
import com.google.cloud.pubsublite.flink.split.SubscriptionPartitionSplit;
import com.google.cloud.pubsublite.internal.BlockingPullSubscriber;
import com.google.cloud.pubsublite.internal.CheckedApiException;
import java.util.Optional;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CompletablePullSubscriberImplTest {
  @Mock BlockingPullSubscriber mockSubscriber;

  static final SubscriptionPartitionSplit split =
      SubscriptionPartitionSplit.create(
          exampleSubscriptionPath(), examplePartition(), Offset.of(0));
  CompletablePullSubscriberImpl subscriber;

  @Test
  public void testOnData() {
    subscriber =
        new CompletablePullSubscriberImpl(
            split, mockSubscriber, PartitionFinishedCondition.continueIndefinitely());
    when(mockSubscriber.onData()).thenReturn(ApiFutures.immediateFuture(null));
    assertThat(subscriber.onData().isDone()).isTrue();
    verify(mockSubscriber).onData();
  }

  @Test
  public void testClose() {
    subscriber =
        new CompletablePullSubscriberImpl(
            split, mockSubscriber, PartitionFinishedCondition.continueIndefinitely());
    subscriber.close();
    verify(mockSubscriber).close();
  }

  @Test
  public void testStopBefore() throws CheckedApiException {

    SequencedMessage message1 = messageFromOffset(Offset.of(0));
    SequencedMessage message2 = messageFromOffset(Offset.of(1));
    SequencedMessage message3 = messageFromOffset(Offset.of(2));
    when(mockSubscriber.messageIfAvailable())
        .thenReturn(Optional.empty())
        .thenReturn(Optional.of(message1))
        .thenReturn(Optional.of(message2))
        .thenReturn(Optional.of(message3));

    subscriber =
        new CompletablePullSubscriberImpl(
            split,
            mockSubscriber,
            (PartitionFinishedCondition)
                (path, partition, message) -> {
                  if (message.offset().value() == 1) {
                    return PartitionFinishedCondition.Result.FINISH_BEFORE;
                  }
                  return PartitionFinishedCondition.Result.CONTINUE;
                });

    assertThat(subscriber.messageIfAvailable()).isEqualTo(Optional.empty());
    assertThat(subscriber.messageIfAvailable()).isEqualTo(Optional.of(message1));
    assertThat(subscriber.isFinished()).isFalse();
    assertThat(subscriber.messageIfAvailable()).isEqualTo(Optional.empty());
    assertThat(subscriber.isFinished()).isTrue();
    assertThat(subscriber.messageIfAvailable()).isEqualTo(Optional.empty());
  }

  @Test
  public void testStopAfter() throws CheckedApiException {
    SequencedMessage message1 = messageFromOffset(Offset.of(0));
    SequencedMessage message2 = messageFromOffset(Offset.of(1));
    SequencedMessage message3 = messageFromOffset(Offset.of(2));
    when(mockSubscriber.messageIfAvailable())
        .thenReturn(Optional.empty())
        .thenReturn(Optional.of(message1))
        .thenReturn(Optional.of(message2))
        .thenReturn(Optional.of(message3));

    subscriber =
        new CompletablePullSubscriberImpl(
            split,
            mockSubscriber,
            (PartitionFinishedCondition)
                (path, partition, message) -> {
                  if (message.offset().value() == 1) {
                    return PartitionFinishedCondition.Result.FINISH_AFTER;
                  }
                  return PartitionFinishedCondition.Result.CONTINUE;
                });

    assertThat(subscriber.messageIfAvailable()).isEqualTo(Optional.empty());
    assertThat(subscriber.messageIfAvailable()).isEqualTo(Optional.of(message1));
    assertThat(subscriber.isFinished()).isFalse();
    assertThat(subscriber.messageIfAvailable()).isEqualTo(Optional.of(message2));
    assertThat(subscriber.isFinished()).isTrue();

    assertThat(subscriber.messageIfAvailable()).isEqualTo(Optional.empty());
  }
}

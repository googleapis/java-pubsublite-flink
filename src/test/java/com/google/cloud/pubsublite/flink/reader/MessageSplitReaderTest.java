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
import static com.google.cloud.pubsublite.flink.TestUtilities.recordWithSplitsToMap;
import static com.google.cloud.pubsublite.internal.testing.UnitTestExamples.exampleSubscriptionPath;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.SequencedMessage;
import com.google.cloud.pubsublite.flink.split.SubscriptionPartitionSplit;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MessageSplitReaderTest {
  @Mock CompletablePullSubscriber mockSubscriber;

  public static SubscriptionPartitionSplit splitFromPartition(Partition partition) {
    return SubscriptionPartitionSplit.create(exampleSubscriptionPath(), partition, Offset.of(0));
  }

  @Test
  public void testEmptyFetch() throws Exception {
    MessageSplitReader reader = new MessageSplitReader((a) -> null);
    RecordsWithSplitIds<SequencedMessage> records = reader.fetch();
    assertThat(records.finishedSplits()).isEmpty();
    assertThat(recordWithSplitsToMap(records)).isEmpty();
  }

  @Test
  public void testFetch() throws Exception {
    SubscriptionPartitionSplit split0 = splitFromPartition(Partition.of(0));
    SubscriptionPartitionSplit split1 = splitFromPartition(Partition.of(1));
    Map<SubscriptionPartitionSplit, CompletablePullSubscriber> subscribers = new HashMap<>();
    subscribers.put(
        split0,
        new FakeSubscriber(
            ImmutableList.of(Optional.empty(), Optional.of(messageFromOffset(Offset.of(0))))));
    subscribers.put(
        split1,
        new FakeSubscriber(
            ImmutableList.of(
                Optional.of(messageFromOffset(Offset.of(1))),
                Optional.of(messageFromOffset(Offset.of(2))),
                Optional.empty())));
    MessageSplitReader reader = new MessageSplitReader(subscribers::get);
    reader.handleSplitsChanges(new SplitsAddition<>(ImmutableList.of(split0, split1)));

    // One split doesn't yet have messages
    RecordsWithSplitIds<SequencedMessage> records = reader.fetch();
    assertThat(records.finishedSplits()).isEmpty();
    assertThat(recordWithSplitsToMap(records))
        .containsExactly(split1.splitId(), messageFromOffset(Offset.of(1)));

    // Both splits have messages, one finishes.
    records = reader.fetch();
    assertThat(records.finishedSplits()).containsExactly(split0.splitId());
    assertThat(recordWithSplitsToMap(records))
        .containsExactly(
            split0.splitId(), messageFromOffset(Offset.of(0)),
            split1.splitId(), messageFromOffset(Offset.of(2)));

    // One remaining split which now finishes.
    records = reader.fetch();
    assertThat(records.finishedSplits()).containsExactly(split1.splitId());
    assertThat(recordWithSplitsToMap(records)).isEmpty();
  }

  @Test
  public void testFetchThrowsError() throws Exception {
    CompletablePullSubscriber mockSubscriber =
        spy(
            new FakeSubscriber(
                ImmutableList.of(
                    Optional.of(messageFromOffset(Offset.of(0))),
                    Optional.of(messageFromOffset(Offset.of(0))))));

    SubscriptionPartitionSplit split0 = splitFromPartition(Partition.of(0));
    Map<SubscriptionPartitionSplit, CompletablePullSubscriber> subscribers =
        ImmutableMap.of(split0, mockSubscriber);
    MessageSplitReader reader = new MessageSplitReader(subscribers::get);
    reader.handleSplitsChanges(new SplitsAddition<>(ImmutableList.of(split0)));

    doThrow(new RuntimeException("error")).when(mockSubscriber).close();

    reader.fetch();
    assertThrows(RuntimeException.class, reader::close);
    verify(mockSubscriber).close();
  }

  @Test
  public void testSplitFinishIgnoresCloseError() throws Exception {
    CompletablePullSubscriber mockSubscriber = spy(new FakeSubscriber(ImmutableList.of()));

    SubscriptionPartitionSplit split0 = splitFromPartition(Partition.of(0));
    Map<SubscriptionPartitionSplit, CompletablePullSubscriber> subscribers =
        ImmutableMap.of(split0, mockSubscriber);
    MessageSplitReader reader = new MessageSplitReader(subscribers::get);
    reader.handleSplitsChanges(new SplitsAddition<>(ImmutableList.of(split0)));

    doThrow(new RuntimeException("error")).when(mockSubscriber).close();

    reader.fetch();

    verify(mockSubscriber).close();
    reset(mockSubscriber);
    reader.close();
    verifyNoMoreInteractions(mockSubscriber);
  }

  @Test
  public void testReaderThrowsUnderlyingErrorOnClose() throws Exception {
    CompletablePullSubscriber mockSubscriber =
        spy(
            new FakeSubscriber(
                ImmutableList.of(
                    Optional.of(messageFromOffset(Offset.of(0))),
                    Optional.of(messageFromOffset(Offset.of(0))))));

    SubscriptionPartitionSplit split0 = splitFromPartition(Partition.of(0));
    Map<SubscriptionPartitionSplit, CompletablePullSubscriber> subscribers =
        ImmutableMap.of(split0, mockSubscriber);
    MessageSplitReader reader = new MessageSplitReader(subscribers::get);
    reader.handleSplitsChanges(new SplitsAddition<>(ImmutableList.of(split0)));

    doThrow(new RuntimeException("bad")).when(mockSubscriber).close();

    reader.fetch();
    assertThrows(RuntimeException.class, reader::close);
    verify(mockSubscriber).close();
  }
}

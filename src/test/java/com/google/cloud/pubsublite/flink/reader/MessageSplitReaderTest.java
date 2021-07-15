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
import static com.google.cloud.pubsublite.internal.testing.UnitTestExamples.examplePartition;
import static com.google.cloud.pubsublite.internal.testing.UnitTestExamples.exampleSubscriptionPath;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode.Code;
import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.SequencedMessage;
import com.google.cloud.pubsublite.flink.split.SubscriptionPartitionSplit;
import com.google.cloud.pubsublite.internal.CheckedApiException;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Optional;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MessageSplitReaderTest {

  static final SubscriptionPartitionSplit exampleSplit = splitFromPartition(examplePartition());
  @Mock CompletablePullSubscriber.Factory mockFactory;
  MessageSplitReader reader;

  @Before
  public void setUp() {
    reader = new MessageSplitReader(mockFactory);
  }

  public static SubscriptionPartitionSplit splitFromPartition(Partition partition) {
    return SubscriptionPartitionSplit.create(exampleSubscriptionPath(), partition, Offset.of(0));
  }

  @Test
  public void testFetch_Empty() throws Exception {
    RecordsWithSplitIds<SequencedMessage> records = reader.fetch();
    assertThat(records.finishedSplits()).isEmpty();
    assertThat(recordWithSplitsToMap(records)).isEmpty();
  }

  @Test
  public void testFetch_SubscribeCreationError() throws Exception {
    when(mockFactory.New(exampleSplit)).thenThrow(new CheckedApiException(Code.INTERNAL));

    assertThrows(
        ApiException.class,
        () -> reader.handleSplitsChanges(new SplitsAddition<>(ImmutableList.of(exampleSplit))));
  }

  @Test
  public void testFetch_MessageIfAnyError() throws CheckedApiException {
    CompletablePullSubscriber mockSubscriber =
        spy(new FakeSubscriber(ImmutableList.of(Optional.empty())));
    doAnswer(
            (invocation) -> {
              throw new CheckedApiException(Code.INTERNAL);
            })
        .when(mockSubscriber)
        .messageIfAvailable();
    when(mockFactory.New(exampleSplit)).thenReturn(mockSubscriber);
    reader.handleSplitsChanges(new SplitsAddition<>(ImmutableList.of(exampleSplit)));

    assertThrows(IOException.class, reader::fetch);
  }

  @Test
  public void testFetch() throws Exception {
    SubscriptionPartitionSplit split0 = splitFromPartition(Partition.of(0));
    SubscriptionPartitionSplit split1 = splitFromPartition(Partition.of(1));
    when(mockFactory.New(split0))
        .thenReturn(
            new FakeSubscriber(
                ImmutableList.of(Optional.empty(), Optional.of(messageFromOffset(Offset.of(0))))));
    when(mockFactory.New(split1))
        .thenReturn(
            new FakeSubscriber(
                ImmutableList.of(
                    Optional.of(messageFromOffset(Offset.of(1))),
                    Optional.of(messageFromOffset(Offset.of(2))),
                    Optional.empty())));
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
  public void testCloseThrowsError() throws Exception {
    CompletablePullSubscriber mockSubscriber =
        spy(new FakeSubscriber(ImmutableList.of(Optional.empty(), Optional.empty())));
    when(mockFactory.New(exampleSplit)).thenReturn(mockSubscriber);
    reader.handleSplitsChanges(new SplitsAddition<>(ImmutableList.of(exampleSplit)));

    doThrow(new RuntimeException("error")).when(mockSubscriber).close();

    reader.fetch();
    assertThrows(RuntimeException.class, reader::close);
    verify(mockSubscriber).close();
  }

  @Test
  public void testSplitFinishIgnoresCloseError() throws Exception {
    CompletablePullSubscriber mockSubscriber =
        spy(new FakeSubscriber(ImmutableList.of(Optional.empty())));
    when(mockFactory.New(exampleSplit)).thenReturn(mockSubscriber);
    reader.handleSplitsChanges(new SplitsAddition<>(ImmutableList.of(exampleSplit)));

    doThrow(new RuntimeException("error")).when(mockSubscriber).close();

    reader.fetch();

    verify(mockSubscriber).close();
    reset(mockSubscriber);
    reader.close();
    verifyNoMoreInteractions(mockSubscriber);
  }

  @Test
  public void testUnknownSplitChange() {
    assertThrows(IllegalArgumentException.class, () -> reader.handleSplitsChanges(null));
  }
}

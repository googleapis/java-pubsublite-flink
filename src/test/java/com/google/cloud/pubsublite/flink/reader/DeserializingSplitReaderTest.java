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
import static com.google.cloud.pubsublite.flink.reader.ReaderUtils.recordWithSplitsToMap;
import static com.google.cloud.pubsublite.internal.testing.UnitTestExamples.examplePartition;
import static com.google.cloud.pubsublite.internal.testing.UnitTestExamples.exampleSubscriptionPath;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.SequencedMessage;
import com.google.cloud.pubsublite.flink.MessageTimestampExtractor;
import com.google.cloud.pubsublite.flink.PubsubLiteDeserializationSchema;
import com.google.cloud.pubsublite.flink.split.SubscriptionPartitionSplit;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import java.util.Optional;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DeserializingSplitReaderTest {
  static final SubscriptionPartitionSplit split =
      SubscriptionPartitionSplit.create(
          exampleSubscriptionPath(), examplePartition(), Offset.of(0));

  @Mock PubsubLiteDeserializationSchema<String> mockDeserializationSchema;
  @Mock MessageTimestampExtractor mockTimestampExtractor;
  @Mock SplitReader<SequencedMessage, SubscriptionPartitionSplit> mockSplitReader;
  DeserializingSplitReader<String> splitReader;

  @Before
  public void setUp() {
    splitReader =
        new DeserializingSplitReader<>(
            mockSplitReader, mockDeserializationSchema, mockTimestampExtractor);
  }

  @Test
  public void testDelegatingMethods() throws Exception {
    SplitsAddition<SubscriptionPartitionSplit> addition =
        new SplitsAddition<>(ImmutableList.of(split));
    splitReader.handleSplitsChanges(addition);
    verify(mockSplitReader).handleSplitsChanges(addition);

    splitReader.close();
    verify(mockSplitReader).close();

    splitReader.wakeUp();
    verify(mockSplitReader).wakeUp();
  }

  @Test
  public void testDeserialization() throws Exception {
    SequencedMessage message1 = messageFromOffset(Offset.of(10));
    SequencedMessage message2 = messageFromOffset(Offset.of(20));

    when(mockDeserializationSchema.deserialize(message1)).thenReturn("one");
    when(mockDeserializationSchema.deserialize(message2)).thenReturn("two");
    when(mockTimestampExtractor.timestampMillis(message1)).thenReturn(1L);
    when(mockTimestampExtractor.timestampMillis(message2)).thenReturn(2L);

    RecordsBySplits.Builder<SequencedMessage> records = new RecordsBySplits.Builder<>();
    records.add("1", message1);
    records.add("2", message2);
    records.addFinishedSplit("finished");
    when(mockSplitReader.fetch()).thenReturn(records.build());

    RecordsBySplits<Record<String>> deserialized = splitReader.fetch();
    assertThat(deserialized.finishedSplits()).containsExactly("finished");
    Multimap<String, Record<String>> messages = recordWithSplitsToMap(deserialized);
    assertThat(messages.get("1"))
        .containsExactly(Record.create(Optional.of("one"), Offset.of(10), 1));
    assertThat(messages.get("2"))
        .containsExactly(Record.create(Optional.of("two"), Offset.of(20), 2));
  }

  @Test
  public void testDeserializationReturnsNull() throws Exception {
    SequencedMessage message1 = messageFromOffset(Offset.of(10));

    when(mockDeserializationSchema.deserialize(message1)).thenReturn(null);
    when(mockTimestampExtractor.timestampMillis(message1)).thenReturn(1L);

    RecordsBySplits.Builder<SequencedMessage> records = new RecordsBySplits.Builder<>();
    records.add("1", message1);
    when(mockSplitReader.fetch()).thenReturn(records.build());

    RecordsBySplits<Record<String>> deserialized = splitReader.fetch();
    assertThat(deserialized.finishedSplits()).isEmpty();
    Multimap<String, Record<String>> messages = recordWithSplitsToMap(deserialized);
    assertThat(messages.get("1"))
        .containsExactly(Record.create(Optional.empty(), Offset.of(10), 1));
  }

  @Test
  public void testDeserializationFailure() throws Exception {
    SequencedMessage message1 = messageFromOffset(Offset.of(10));

    when(mockDeserializationSchema.deserialize(message1)).thenThrow(new Exception(""));

    RecordsBySplits.Builder<SequencedMessage> records = new RecordsBySplits.Builder<>();
    records.add("1", message1);
    when(mockSplitReader.fetch()).thenReturn(records.build());

    assertThrows(Exception.class, () -> splitReader.fetch());
  }

  @Test
  public void testTimestampFailure() throws Exception {
    SequencedMessage message1 = messageFromOffset(Offset.of(10));

    when(mockDeserializationSchema.deserialize(message1)).thenReturn("one");
    when(mockTimestampExtractor.timestampMillis(message1)).thenThrow(new Exception(""));

    RecordsBySplits.Builder<SequencedMessage> records = new RecordsBySplits.Builder<>();
    records.add("1", message1);
    when(mockSplitReader.fetch()).thenReturn(records.build());

    assertThrows(Exception.class, () -> splitReader.fetch());
  }
}

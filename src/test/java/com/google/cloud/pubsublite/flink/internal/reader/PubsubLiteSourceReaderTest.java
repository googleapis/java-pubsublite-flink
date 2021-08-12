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
package com.google.cloud.pubsublite.flink.internal.reader;

import static com.google.cloud.pubsublite.internal.testing.UnitTestExamples.exampleSubscriptionPath;
import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFutures;
import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.SequencedMessage;
import com.google.cloud.pubsublite.flink.MessageTimestampExtractor;
import com.google.cloud.pubsublite.flink.PubsubLiteDeserializationSchema;
import com.google.cloud.pubsublite.flink.internal.split.SubscriptionPartitionSplit;
import com.google.cloud.pubsublite.internal.CursorClient;
import com.google.cloud.pubsublite.proto.Cursor;
import com.google.cloud.pubsublite.proto.PubSubMessage;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.util.Optional;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.testutils.source.reader.TestingReaderOutput;
import org.apache.flink.core.io.InputStatus;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PubsubLiteSourceReaderTest {
  @Mock CompletablePullSubscriber.Factory mockFactory;
  @Mock CursorClient mockCursorClient;
  @Mock SourceReaderContext mockContext;
  TestingReaderOutput<String> output = new TestingReaderOutput<>();
  SourceReader<String, SubscriptionPartitionSplit> reader;

  public static CompletablePullSubscriber subscriberFromIntegers(int... messages) {
    ImmutableList.Builder<Optional<SequencedMessage>> builder = ImmutableList.builder();
    for (int i : messages) {
      SequencedMessage message =
          SequencedMessage.fromProto(
              com.google.cloud.pubsublite.proto.SequencedMessage.newBuilder()
                  .setMessage(
                      PubSubMessage.newBuilder().setData(ByteString.copyFromUtf8(Long.toString(i))))
                  .setCursor(Cursor.newBuilder().setOffset(i))
                  .build());
      builder.add(Optional.of(message));
    }
    return new FakeSubscriber(builder.build());
  }

  public static SubscriptionPartitionSplit makeSplit(Partition partition, Offset offset) {
    return SubscriptionPartitionSplit.create(exampleSubscriptionPath(), partition, offset);
  }

  @Before
  public void setUp() {
    reader =
        new PubsubLiteSourceReader<>(
            new PubsubLiteRecordEmitter<>(),
            mockCursorClient,
            () ->
                new DeserializingSplitReader<>(
                    new MessageSplitReader(mockFactory),
                    PubsubLiteDeserializationSchema.dataOnly(new SimpleStringSchema()),
                    MessageTimestampExtractor.publishTimeExtractor()),
            new Configuration(),
            mockContext);
    when(mockCursorClient.commitCursor(any(), any(), any()))
        .thenReturn(ApiFutures.immediateFuture(null));
  }

  @Test
  public void testReader() throws Exception {
    SubscriptionPartitionSplit split0 = makeSplit(Partition.of(0), Offset.of(0));
    SubscriptionPartitionSplit split1 = makeSplit(Partition.of(1), Offset.of(0));
    when(mockFactory.New(split0)).thenReturn(subscriberFromIntegers(0, 2));
    when(mockFactory.New(split1)).thenReturn(subscriberFromIntegers(1, 3, 5, 7));

    reader.addSplits(ImmutableList.of(split0, split1));
    while (output.getEmittedRecords().size() < 4) {
      reader.pollNext(output);
    }
    assertThat(output.getEmittedRecords()).containsExactly("0", "1", "2", "3");

    reader.snapshotState(1);
    reader.notifyCheckpointComplete(1);
    verify(mockCursorClient).commitCursor(exampleSubscriptionPath(), Partition.of(0), Offset.of(2));
    verify(mockCursorClient).commitCursor(exampleSubscriptionPath(), Partition.of(1), Offset.of(3));

    while (output.getEmittedRecords().size() < 6) {
      reader.pollNext(output);
    }
    assertThat(reader.pollNext(output)).isEqualTo(InputStatus.NOTHING_AVAILABLE);

    reader.notifyNoMoreSplits();
    while (reader.pollNext(output) != InputStatus.END_OF_INPUT) {
      assertThat(output.getEmittedRecords()).hasSize(6);
    }
    assertThat(reader.pollNext(output)).isEqualTo(InputStatus.END_OF_INPUT);
    assertThat(output.getEmittedRecords()).containsExactly("0", "1", "2", "3", "5", "7");
  }
}

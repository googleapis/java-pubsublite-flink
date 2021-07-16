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

import static com.google.cloud.pubsublite.internal.testing.UnitTestExamples.*;
import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.*;

import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.flink.split.SubscriptionPartitionSplit;
import com.google.cloud.pubsublite.flink.split.SubscriptionPartitionSplitState;
import java.time.Instant;
import java.util.Optional;
import org.apache.flink.api.connector.source.SourceOutput;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PubsubLiteRecordEmitterTest {
  static final SubscriptionPartitionSplit split =
      SubscriptionPartitionSplit.create(
          exampleSubscriptionPath(), examplePartition(), Offset.of(0));
  static final Instant exampleTime = Instant.ofEpochMilli(1000);

  @Mock SourceOutput<Long> mockOutput;

  @Test
  public void testRecordEmitted() {
    SubscriptionPartitionSplitState state = new SubscriptionPartitionSplitState(split);
    PubsubLiteRecordEmitter<Long> emitter = new PubsubLiteRecordEmitter<>();

    emitter.emitRecord(
        Record.create(Optional.of(100L), exampleOffset(), exampleTime), mockOutput, state);

    assertThat(state.toSplit().start()).isEqualTo(exampleOffset());
    verify(mockOutput).collect(100L, exampleTime.toEpochMilli());
  }

  @Test
  public void testNullRecordAdvancesOffset() {
    SubscriptionPartitionSplitState state = new SubscriptionPartitionSplitState(split);
    PubsubLiteRecordEmitter<Long> emitter = new PubsubLiteRecordEmitter<>();

    emitter.emitRecord(
        Record.create(Optional.empty(), exampleOffset(), exampleTime), mockOutput, state);

    assertThat(state.toSplit().start()).isEqualTo(exampleOffset());
    verifyNoInteractions(mockOutput);
  }
}

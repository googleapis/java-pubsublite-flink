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
package com.google.cloud.pubsublite.flink.internal.source.reader;

import static com.google.cloud.pubsublite.internal.testing.UnitTestExamples.examplePartition;
import static com.google.cloud.pubsublite.internal.testing.UnitTestExamples.exampleSubscriptionPath;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFutures;
import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.flink.internal.source.split.SubscriptionPartitionSplit;
import com.google.cloud.pubsublite.internal.wire.Committer;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CheckpointCursorCommitterTest {
  @Mock CommitterFactory mockCommitterFactory;
  @Mock Committer mockCommitter;
  CheckpointCursorCommitter cursorCommitter;

  @Before
  public void setUp() {
    cursorCommitter = new CheckpointCursorCommitter(mockCommitterFactory);
    when(mockCommitterFactory.getCommitter(any())).thenReturn(mockCommitter);
    when(mockCommitter.commitOffset(any())).thenReturn(ApiFutures.immediateFuture(null));
  }

  public static SubscriptionPartitionSplit splitFromPartition(Partition partition) {
    return SubscriptionPartitionSplit.create(exampleSubscriptionPath(), partition, Offset.of(0));
  }

  @Test
  public void testFinishedSplits() {
    SubscriptionPartitionSplit split = splitFromPartition(examplePartition());
    cursorCommitter.notifySplitFinished(ImmutableList.of(split));
    cursorCommitter.addCheckpoint(1, ImmutableList.of());
    cursorCommitter.notifyCheckpointComplete(1);
    verify(mockCommitterFactory).getCommitter(split.partition());
    verify(mockCommitter).commitOffset(split.start());
  }

  @Test
  public void testCheckpointCommitted() {
    SubscriptionPartitionSplit split = splitFromPartition(examplePartition());
    cursorCommitter.addCheckpoint(1, ImmutableList.of(split));
    cursorCommitter.notifyCheckpointComplete(1);
    verify(mockCommitterFactory).getCommitter(split.partition());
    verify(mockCommitter).commitOffset(split.start());
  }

  @Test
  public void testUnknownCheckpoint() {
    SubscriptionPartitionSplit split = splitFromPartition(examplePartition());
    cursorCommitter.addCheckpoint(1, ImmutableList.of(split));
    cursorCommitter.notifyCheckpointComplete(4);
    verifyNoInteractions(mockCommitter);
  }

  @Test
  public void testIntermediateCheckpointSkipped() {
    SubscriptionPartitionSplit split1 = splitFromPartition(Partition.of(1));
    SubscriptionPartitionSplit split2 = splitFromPartition(Partition.of(2));
    // The numeric ids of the checkpoints don't matter, just the order they're taken in.
    cursorCommitter.addCheckpoint(2, ImmutableList.of(split2));
    cursorCommitter.addCheckpoint(1, ImmutableList.of(split1));

    // Checkpoint 1 is committed, removing checkpoint 2
    cursorCommitter.notifyCheckpointComplete(1);
    verify(mockCommitterFactory).getCommitter(split1.partition());
    verify(mockCommitter).commitOffset(split1.start());
    cursorCommitter.notifyCheckpointComplete(2);
    verifyNoMoreInteractions(mockCommitter);
  }

  @Test
  public void testClose() {
    cursorCommitter.close();
    verify(mockCommitterFactory).close();
  }
}

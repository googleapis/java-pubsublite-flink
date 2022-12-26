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
package com.google.cloud.pubsublite.flink.internal.source.enumerator;

import static com.google.cloud.pubsublite.internal.testing.UnitTestExamples.exampleOffset;
import static com.google.cloud.pubsublite.internal.testing.UnitTestExamples.exampleSubscriptionPath;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.flink.internal.source.split.SubscriptionPartitionSplit;
import com.google.cloud.pubsublite.flink.proto.SplitEnumeratorCheckpoint;
import com.google.cloud.pubsublite.flink.proto.SplitEnumeratorCheckpoint.Discovery;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Multimap;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import org.apache.flink.connector.testutils.source.reader.TestingSplitEnumeratorContext;
import org.apache.flink.connector.testutils.source.reader.TestingSplitEnumeratorContext.SplitAssignmentState;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PubsubLiteSplitEnumeratorTest {
  private final TestingSplitEnumeratorContext<SubscriptionPartitionSplit> testContext =
      new TestingSplitEnumeratorContext<>(2);
  @Mock SplitDiscovery discovery;
  private final PartitionAssigner assigner = UniformPartitionAssigner.create();

  static SubscriptionPartitionSplit makeSplit(Partition partition) {
    return SubscriptionPartitionSplit.create(exampleSubscriptionPath(), partition, exampleOffset());
  }

  public PubsubLiteSplitEnumerator createEnumerator() {
    return new PubsubLiteSplitEnumerator(testContext, assigner, discovery);
  }

  static Multimap<Integer, SubscriptionPartitionSplit> toMap(
      Map<Integer, SplitAssignmentState<SubscriptionPartitionSplit>> state) {
    ImmutableListMultimap.Builder<Integer, SubscriptionPartitionSplit> builder =
        ImmutableListMultimap.builder();
    state.forEach((k, v) -> builder.putAll(k, v.getAssignedSplits()));
    return builder.build();
  }

  static boolean anyFinished(Map<Integer, SplitAssignmentState<SubscriptionPartitionSplit>> state) {
    for (SplitAssignmentState<SubscriptionPartitionSplit> assignment : state.values()) {
      if (assignment.hasReceivedNoMoreSplitsSignal()) return true;
    }
    return false;
  }

  static boolean allFinished(Map<Integer, SplitAssignmentState<SubscriptionPartitionSplit>> state) {
    for (SplitAssignmentState<SubscriptionPartitionSplit> assignment : state.values()) {
      if (!assignment.hasReceivedNoMoreSplitsSignal()) return false;
    }
    return true;
  }

  // The test context contains a manually triggered executor service. This helper iterates over
  // all tasks which have been scheduled by the executor service and throws any exceptions they
  // encountered.
  public void throwAnyTaskExceptions() throws Exception {
    for (ScheduledFuture<?> task : testContext.getExecutorService().getScheduledTasks()) {
      if (task.isDone()) {
        task.get();
      }
    }
  }

  @Test
  public void testClose() throws Exception {
    PubsubLiteSplitEnumerator enumerator = createEnumerator();
    enumerator.close();
    verify(discovery).close();
  }

  @Test
  public void testClose_Exception() throws Exception {
    PubsubLiteSplitEnumerator enumerator = createEnumerator();
    doThrow(new Exception("error")).when(discovery).close();
    assertThrows(IOException.class, enumerator::close);
  }

  @Test
  public void testSplitDiscovery_EmptyPoll() {
    PubsubLiteSplitEnumerator enumerator = createEnumerator();
    enumerator.start();

    when(discovery.discoverNewSplits()).thenReturn(ImmutableList.of());
    testContext.triggerAllActions();

    assertThat(toMap(testContext.getSplitAssignments())).isEmpty();
  }

  @Test
  public void testSplitDiscovery_InitialError() {
    PubsubLiteSplitEnumerator enumerator = createEnumerator();
    enumerator.start();

    when(discovery.discoverNewSplits()).thenThrow(new RuntimeException("error"));
    testContext.triggerAllActions();

    assertThat(toMap(testContext.getSplitAssignments())).isEmpty();

    assertThrows(ExecutionException.class, this::throwAnyTaskExceptions);
  }

  @Test
  public void testSplitDiscovery_ErrorAfterInitialAssignment() throws Exception {
    PubsubLiteSplitEnumerator enumerator = createEnumerator();
    enumerator.start();

    SubscriptionPartitionSplit s0 = makeSplit(Partition.of(0));

    when(discovery.discoverNewSplits()).thenReturn(ImmutableList.of(s0));
    testContext.triggerAllActions();

    when(discovery.discoverNewSplits()).thenThrow(new RuntimeException("error"));
    testContext.triggerAllActions();

    assertThat(toMap(testContext.getSplitAssignments())).isEmpty();

    throwAnyTaskExceptions();
  }

  @Test
  public void testCheckpoint() {
    PubsubLiteSplitEnumerator enumerator = createEnumerator();

    when(discovery.checkpoint()).thenReturn(Discovery.newBuilder().build());
    SplitEnumeratorCheckpoint checkpoint = enumerator.snapshotState();
    assertThat(checkpoint.getAssignmentsList()).containsExactlyElementsIn(assigner.checkpoint());
    assertThat(checkpoint.getDiscovery()).isEqualTo(discovery.checkpoint());
  }

  @Test
  public void testContinuousSplitDiscovery() {
    PubsubLiteSplitEnumerator enumerator = createEnumerator();
    enumerator.start();

    SubscriptionPartitionSplit s0 = makeSplit(Partition.of(0));
    SubscriptionPartitionSplit s1 = makeSplit(Partition.of(1));

    testContext.registerReader(0, "h0");
    when(discovery.discoverNewSplits()).thenReturn(ImmutableList.of(s0));
    testContext.triggerAllActions();

    assertThat(toMap(testContext.getSplitAssignments())).containsExactly(0, s0);
    assertThat(anyFinished(testContext.getSplitAssignments())).isFalse();

    testContext.registerReader(1, "h1");
    when(discovery.discoverNewSplits()).thenReturn(ImmutableList.of(s1));
    testContext.triggerAllActions();

    assertThat(toMap(testContext.getSplitAssignments())).containsExactly(0, s0, 1, s1);
    assertThat(anyFinished(testContext.getSplitAssignments())).isFalse();
  }

  @Test
  public void testContinuousSplitDiscovery_DelayedReaderRegistration() {
    PubsubLiteSplitEnumerator enumerator = createEnumerator();
    enumerator.start();

    SubscriptionPartitionSplit s0 = makeSplit(Partition.of(0));
    SubscriptionPartitionSplit s1 = makeSplit(Partition.of(1));

    when(discovery.discoverNewSplits()).thenReturn(ImmutableList.of(s0));
    testContext.triggerAllActions();

    assertThat(testContext.getSplitAssignments()).isEmpty();

    testContext.registerReader(0, "h0");
    testContext.registerReader(1, "h1");
    when(discovery.discoverNewSplits()).thenReturn(ImmutableList.of(s1));
    testContext.triggerAllActions();

    assertThat(toMap(testContext.getSplitAssignments())).containsExactly(0, s0, 1, s1);
    assertThat(anyFinished(testContext.getSplitAssignments())).isFalse();
  }

  @Test
  public void testContinuousReaderRegistration() {
    PubsubLiteSplitEnumerator enumerator = createEnumerator();
    enumerator.start();

    SubscriptionPartitionSplit s0 = makeSplit(Partition.of(0));

    when(discovery.discoverNewSplits()).thenReturn(ImmutableList.of(s0));
    testContext.triggerAllActions();
    assertThat(testContext.getSplitAssignments()).isEmpty();

    testContext.registerReader(0, "h0");
    enumerator.addReader(0);
    assertThat(toMap(testContext.getSplitAssignments())).containsExactly(0, s0);
    assertThat(anyFinished(testContext.getSplitAssignments())).isFalse();
  }

  @Test
  public void testContinuousAddSplitBack() {
    PubsubLiteSplitEnumerator enumerator = createEnumerator();

    SubscriptionPartitionSplit s0 = makeSplit(Partition.of(0));

    testContext.registerReader(0, "h0");
    enumerator.addSplitsBack(ImmutableList.of(s0), 1);

    assertThat(toMap(testContext.getSplitAssignments())).containsExactly(0, s0);
    assertThat(anyFinished(testContext.getSplitAssignments())).isFalse();
  }
}

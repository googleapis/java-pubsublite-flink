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
package com.google.cloud.pubsublite.flink.enumerator;

import static com.google.cloud.pubsublite.internal.testing.UnitTestExamples.exampleOffset;
import static com.google.cloud.pubsublite.internal.testing.UnitTestExamples.exampleSubscriptionPath;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.flink.proto.SplitEnumeratorCheckpoint.Assignment;
import com.google.cloud.pubsublite.flink.split.SubscriptionPartitionSplit;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class UniformPartitionAssignerTest {

  PartitionAssigner assigner = UniformPartitionAssigner.create();

  static SubscriptionPartitionSplit makeSplit(Partition partition) {
    return SubscriptionPartitionSplit.create(exampleSubscriptionPath(), partition, exampleOffset());
  }

  @Test
  public void testListSplits() {
    SubscriptionPartitionSplit s0 = makeSplit(Partition.of(0));
    SubscriptionPartitionSplit s1 = makeSplit(Partition.of(1));
    SubscriptionPartitionSplit s2 = makeSplit(Partition.of(2));

    assertThat(assigner.listSplits()).isEmpty();

    assigner.addSplits(ImmutableList.of(s0, s1, s2));
    assertThat(assigner.listSplits()).containsExactly(s0, s1, s2);
  }

  @Test
  public void testInvalidParallelism() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          assigner.assignSplitsForTasks(ImmutableList.of(), 0);
        });
  }

  @Test
  public void testSimpleAssignments() {
    SubscriptionPartitionSplit s0 = makeSplit(Partition.of(0));
    SubscriptionPartitionSplit s1 = makeSplit(Partition.of(1));

    Map<TaskId, List<SubscriptionPartitionSplit>> assignments;
    assigner.addSplits(ImmutableList.of(s0, s1));
    assignments = assigner.assignSplitsForTasks(ImmutableList.of(TaskId.of(0), TaskId.of(1)), 2);

    assertThat(assignments.get(TaskId.of(0))).containsExactly(s0);
    assertThat(assignments.get(TaskId.of(1))).containsExactly(s1);

    assignments = assigner.assignSplitsForTasks(ImmutableList.of(TaskId.of(0), TaskId.of(1)), 2);

    assertThat(assignments.get(TaskId.of(0))).isEmpty();
    assertThat(assignments.get(TaskId.of(1))).isEmpty();
  }

  @Test
  public void testIncrementalAssignments() {
    SubscriptionPartitionSplit s0 = makeSplit(Partition.of(0));
    SubscriptionPartitionSplit s1 = makeSplit(Partition.of(1));

    assigner.addSplits(ImmutableList.of(s0, s1));
    Map<TaskId, List<SubscriptionPartitionSplit>> assignments;

    assignments = assigner.assignSplitsForTasks(ImmutableList.of(TaskId.of(0)), 2);
    assertThat(assignments.get(TaskId.of(0))).containsExactly(s0);

    assignments = assigner.assignSplitsForTasks(ImmutableList.of(TaskId.of(0), TaskId.of(1)), 2);
    assertThat(assignments.get(TaskId.of(0))).isEmpty();
    assertThat(assignments.get(TaskId.of(1))).containsExactly(s1);
  }

  @Test
  public void testCheckpointRestore() {
    SubscriptionPartitionSplit s0 = makeSplit(Partition.of(0));
    SubscriptionPartitionSplit s1 = makeSplit(Partition.of(1));

    assigner.addSplits(ImmutableList.of(s0, s1));
    Map<TaskId, List<SubscriptionPartitionSplit>> assignments;

    assignments = assigner.assignSplitsForTasks(ImmutableList.of(TaskId.of(0)), 2);
    assertThat(assignments.get(TaskId.of(0))).containsExactly(s0);

    List<Assignment> checkpoint = assigner.checkpoint();

    assigner = UniformPartitionAssigner.fromCheckpoint(checkpoint);

    assignments = assigner.assignSplitsForTasks(ImmutableList.of(TaskId.of(0), TaskId.of(1)), 2);
    assertThat(assignments.get(TaskId.of(0))).isEmpty();
    assertThat(assignments.get(TaskId.of(1))).containsExactly(s1);
  }

  @Test
  public void testAddSplitTwice() {
    SubscriptionPartitionSplit s0 = makeSplit(Partition.of(0));
    SubscriptionPartitionSplit s1 =
        SubscriptionPartitionSplit.create(
            s0.subscriptionPath(), s0.partition(), Offset.of(s0.start().value() + 1));

    assigner.addSplits(ImmutableList.of(s0));
    assigner.addSplits(ImmutableList.of(s1));

    Map<TaskId, List<SubscriptionPartitionSplit>> assignments;
    assignments = assigner.assignSplitsForTasks(ImmutableList.of(TaskId.of(0)), 1);

    assertThat(assignments.get(TaskId.of(0))).containsExactly(s1);
  }

  @Test
  public void testAssignSplitTwice() {
    SubscriptionPartitionSplit s0 = makeSplit(Partition.of(0));
    SubscriptionPartitionSplit s1 =
        SubscriptionPartitionSplit.create(
            s0.subscriptionPath(), s0.partition(), Offset.of(s0.start().value() + 1));

    Map<TaskId, List<SubscriptionPartitionSplit>> assignments;

    assigner.addSplits(ImmutableList.of(s0));
    assignments = assigner.assignSplitsForTasks(ImmutableList.of(TaskId.of(0)), 1);
    assertThat(assignments.get(TaskId.of(0))).containsExactly(s0);

    assigner.addSplits(ImmutableList.of(s1));
    assignments = assigner.assignSplitsForTasks(ImmutableList.of(TaskId.of(0)), 1);
    assertThat(assignments.get(TaskId.of(0))).containsExactly(s1);
  }

  @Test
  public void testIncrementalAssignmentDistributions() {
    Map<TaskId, List<SubscriptionPartitionSplit>> assignments1, assignments2;
    List<SubscriptionPartitionSplit> splits1 = new ArrayList<>(), splits2 = new ArrayList<>();

    for (int i = 0; i < 100; i++) {
      splits1.add(makeSplit(Partition.of(i)));
    }
    assigner.addSplits(splits1);

    assignments1 = assigner.assignSplitsForTasks(ImmutableList.of(TaskId.of(0), TaskId.of(1)), 2);
    assertThat(assignments1.get(TaskId.of(0))).hasSize(50);
    assertThat(assignments1.get(TaskId.of(1))).hasSize(50);

    for (int i = 100; i < 300; i++) {
      splits2.add(makeSplit(Partition.of(i)));
    }

    assigner.addSplits(splits2);
    assignments2 =
        assigner.assignSplitsForTasks(
            ImmutableList.of(TaskId.of(0), TaskId.of(1), TaskId.of(2)), 3);
    // Since task three has no assignments from the previous round, in order to reach a uniform
    // distribution we will assign fifty more splits to it than to the other tasks.
    assertThat(assignments2.get(TaskId.of(0))).hasSize(50);
    assertThat(assignments2.get(TaskId.of(1))).hasSize(50);
    assertThat(assignments2.get(TaskId.of(2))).hasSize(100);
  }
}

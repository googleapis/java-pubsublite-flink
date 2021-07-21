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

import com.google.auto.value.AutoValue;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.flink.proto.SplitEnumeratorCheckpoint;
import com.google.cloud.pubsublite.flink.split.SubscriptionPartitionSplit;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.*;
import java.util.stream.Collectors;

public class UniformPartitionAssigner implements PartitionAssigner {

  private final HashMap<SplitKey, Integer> assignments;
  private final HashMap<SplitKey, SubscriptionPartitionSplit> allSplits;

  @AutoValue
  abstract static class SplitKey {
    public abstract SubscriptionPath path();

    public abstract Partition partition();

    public static SplitKey of(SubscriptionPartitionSplit split) {
      return new AutoValue_UniformPartitionAssigner_SplitKey(
          split.subscriptionPath(), split.partition());
    }
  }

  static class TaskAndCount {
    final int task;
    final long count;

    public TaskAndCount(int task, long count) {
      this.task = task;
      this.count = count;
    }
  }

  private UniformPartitionAssigner(
      Map<SplitKey, Integer> assignments, Map<SplitKey, SubscriptionPartitionSplit> splits) {
    this.assignments = new HashMap<>(assignments);
    this.allSplits = new HashMap<>(splits);
  }

  static UniformPartitionAssigner create() {
    return new UniformPartitionAssigner(new HashMap<>(), new HashMap<>());
  }

  static UniformPartitionAssigner fromCheckpoint(
      Collection<SplitEnumeratorCheckpoint.Assignment> assignments) {
    Map<SplitKey, Integer> enactedAssignments = new HashMap<>();
    Map<SplitKey, SubscriptionPartitionSplit> splits = new HashMap<>();
    assignments.forEach(
        assignment -> {
          SubscriptionPartitionSplit split =
              SubscriptionPartitionSplit.fromProto(assignment.getSplit());
          SplitKey key = SplitKey.of(split);
          splits.put(key, split);
          if (assignment.hasSubtask()) {
            enactedAssignments.put(key, assignment.getSubtask().getId());
          }
        });
    return new UniformPartitionAssigner(enactedAssignments, splits);
  }

  public List<SplitEnumeratorCheckpoint.Assignment> checkpoint() {
    List<SplitEnumeratorCheckpoint.Assignment> splits = new ArrayList<>();
    allSplits.forEach(
        (key, split) -> {
          SplitEnumeratorCheckpoint.Assignment.Builder b =
              SplitEnumeratorCheckpoint.Assignment.newBuilder();
          b.setSplit(split.toProto());
          if (assignments.containsKey(key)) {
            b.setSubtask(
                SplitEnumeratorCheckpoint.Subtask.newBuilder().setId(assignments.get(key)));
          }
          splits.add(b.build());
        });
    return splits;
  }

  public Map<Integer, List<SubscriptionPartitionSplit>> assignSplitsForTasks(
      Collection<Integer> tasks, int currentParallelism) {
    Multimap<Integer, SplitKey> proposed = computeNewAssignments(currentParallelism);
    Map<Integer, List<SubscriptionPartitionSplit>> newAssignments = new HashMap<>();
    tasks.forEach(
        subtaskId -> {
          Collection<SplitKey> toAssign = proposed.get(subtaskId);
          for (SplitKey key : toAssign) {
            assignments.put(key, subtaskId);
          }
          newAssignments.put(
              subtaskId, toAssign.stream().map(allSplits::get).collect(Collectors.toList()));
        });
    return newAssignments;
  }

  public void addSplits(Collection<SubscriptionPartitionSplit> splits) {
    for (SubscriptionPartitionSplit split : splits) {
      // Use the newer version of the split.
      allSplits.put(SplitKey.of(split), split);
      // Remove any current assignment.
      assignments.remove(SplitKey.of(split));
    }
  }

  public Collection<SubscriptionPartitionSplit> listSplits() {
    return allSplits.values();
  }

  private Multimap<Integer, SplitKey> computeNewAssignments(int numWorkers) {
    HashMap<Integer, Long> taskToCount = new HashMap<>();
    Set<SplitKey> unassigned = allSplits.keySet();
    assignments.forEach(
        (key, task) -> {
          taskToCount.put(task, taskToCount.getOrDefault(task, 0L) + 1);
          unassigned.remove(key);
        });
    // Create a priority queue which orders first by assignment count and second by task index.
    // Ordering by task index isn't important for the distribution, but it makes assignments stable.
    PriorityQueue<TaskAndCount> queue =
        new PriorityQueue<>(
            numWorkers,
            (o1, o2) -> {
              if (o1.count == o2.count) {
                return o1.task - o2.task;
              }
              return Long.signum(o1.count - o2.count);
            });
    // Add each worker to the priority queue with the number of splits they are currently assigned.
    for (int i = 0; i < numWorkers; i++) {
      queue.add(new TaskAndCount(i, taskToCount.getOrDefault(i, 0L)));
    }

    Multimap<Integer, SplitKey> proposal = HashMultimap.create();
    for (SplitKey split : unassigned) {
      TaskAndCount assignment = queue.poll();
      assert assignment != null;
      proposal.put(assignment.task, split);
      queue.add(new TaskAndCount(assignment.task, assignment.count + 1));
    }
    return proposal;
  }
}

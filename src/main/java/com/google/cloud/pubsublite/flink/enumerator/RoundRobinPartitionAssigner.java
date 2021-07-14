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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RoundRobinPartitionAssigner implements PartitionAssigner {

  private static final Logger LOG = LoggerFactory.getLogger(RoundRobinPartitionAssigner.class);
  private final HashMap<SplitKey, Integer> assignments;
  private final HashMap<SplitKey, SubscriptionPartitionSplit> allSplits;

  @AutoValue
  abstract static class SplitKey {
    public abstract SubscriptionPath path();

    public abstract Partition partition();

    public static SplitKey of(SubscriptionPartitionSplit split) {
      return new AutoValue_RoundRobinPartitionAssigner_SplitKey(
          split.subscriptionPath(), split.partition());
    }
  }

  private RoundRobinPartitionAssigner(
      Map<SplitKey, Integer> assignments, Map<SplitKey, SubscriptionPartitionSplit> splits) {
    this.assignments = new HashMap<>(assignments);
    this.allSplits = new HashMap<>(splits);
  }

  static RoundRobinPartitionAssigner create() {
    return new RoundRobinPartitionAssigner(new HashMap<>(), new HashMap<>());
  }

  static RoundRobinPartitionAssigner fromCheckpoint(
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
    return new RoundRobinPartitionAssigner(enactedAssignments, splits);
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
    HashMultimap<Integer, SplitKey> proposal = HashMultimap.create();
    Set<SplitKey> unassigned = allSplits.keySet();
    unassigned.removeAll(assignments.keySet());
    for (SplitKey split : unassigned) {
      int owner = (int) (split.partition().value() % numWorkers);
      proposal.put(owner, split);
    }
    return proposal;
  }
}

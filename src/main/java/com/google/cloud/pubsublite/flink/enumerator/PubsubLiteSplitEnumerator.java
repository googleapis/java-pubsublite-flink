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

import com.google.cloud.pubsublite.flink.proto.SplitEnumeratorCheckpoint;
import com.google.cloud.pubsublite.flink.split.SubscriptionPartitionSplit;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PubsubLiteSplitEnumerator
    implements SplitEnumerator<SubscriptionPartitionSplit, SplitEnumeratorCheckpoint> {

  private static final Logger LOG = LoggerFactory.getLogger(PubsubLiteSplitEnumerator.class);
  private static final Duration PARTITION_DISCOVERY_INTERVAL = Duration.ofMinutes(1);
  private final SplitEnumeratorContext<SubscriptionPartitionSplit> context;
  private final PartitionAssigner assigner;
  private final SplitDiscovery discovery;
  private final Boundedness boundedness;

  public PubsubLiteSplitEnumerator(
      SplitEnumeratorContext<SubscriptionPartitionSplit> context,
      PartitionAssigner assigner,
      SplitDiscovery discovery,
      Boundedness boundedness) {
    this.context = context;
    this.boundedness = boundedness;
    this.assigner = assigner;
    this.discovery = discovery;
  }

  @Override
  public void start() {
    switch (boundedness) {
      case CONTINUOUS_UNBOUNDED:
        this.context.callAsync(
            this::discoverNewSplits,
            this::handlePartitionSplitDiscovery,
            0,
            PARTITION_DISCOVERY_INTERVAL.toMillis());
        break;
      case BOUNDED:
        if (assigner.listSplits().isEmpty()) {
          this.context.callAsync(this::discoverNewSplits, this::handlePartitionSplitDiscovery);
        }
        break;
    }
  }

  @Override
  public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {}

  @Override
  public void addSplitsBack(List<SubscriptionPartitionSplit> splits, int subtaskId) {
    LOG.info("Splits (from reader {}) added back: {}", subtaskId, splits);
    assigner.addSplits(splits);
    updateAssignmentsForRegisteredReaders();
  }

  @Override
  public void addReader(int subtaskId) {
    // We don't need the subtask since we'll just try to figure out if any registered
    // readers should be assigned more splits.
    updateAssignmentsForRegisteredReaders();
  }

  @Override
  public SplitEnumeratorCheckpoint snapshotState(long l) {
    SplitEnumeratorCheckpoint.Builder builder = SplitEnumeratorCheckpoint.newBuilder();
    builder.addAllAssignments(assigner.checkpoint());
    builder.setDiscovery(discovery.checkpoint());
    return builder.build();
  }

  @Override
  public void close() throws IOException {
    try {
      discovery.close();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private List<SubscriptionPartitionSplit> discoverNewSplits() {
    return discovery.discoverNewSplits();
  }

  private void handlePartitionSplitDiscovery(List<SubscriptionPartitionSplit> splits, Throwable t) {
    if (t != null && assigner.listSplits().isEmpty()) {
      // If this was the first split discovery and it failed, throw an error
      throw new RuntimeException(t);
    } else if (t != null) {
      LOG.error("Failed to poll for new splits, continuing", t);
      return;
    }
    if (splits.isEmpty()) {
      return;
    }
    LOG.info("Discovered splits: {}", splits);
    assigner.addSplits(splits);
    updateAssignmentsForRegisteredReaders();
  }

  private void updateAssignmentsForRegisteredReaders() {
    List<TaskId> readers =
        context.registeredReaders().keySet().stream().map(TaskId::of).collect(Collectors.toList());
    Map<TaskId, List<SubscriptionPartitionSplit>> assignment =
        assigner.assignSplitsForTasks(readers, context.currentParallelism());
    LOG.info("Assigning splits: {}", assignment);
    context.assignSplits(
        new SplitsAssignment<>(
            assignment.entrySet().stream()
                .collect(Collectors.toMap(e -> e.getKey().value(), Entry::getValue))));

    // If this is a bounded split enumerator, and we have discovered splits, inform any task which
    // received an assignment that this assignment will be the last.
    if (boundedness == Boundedness.BOUNDED && !assigner.listSplits().isEmpty()) {
      for (TaskId task : assignment.keySet()) {
        context.signalNoMoreSplits(task.value());
      }
    }
  }
}

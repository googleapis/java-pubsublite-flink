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

import com.google.cloud.pubsublite.flink.split.SubscriptionPartitionSplit;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Consumer;

public class CheckpointCursorCommitter {
  @GuardedBy("this")
  private final Set<SubscriptionPartitionSplit> finished = new HashSet<>();

  @GuardedBy("this")
  private final LinkedHashMap<Long, List<SubscriptionPartitionSplit>> checkpoints =
      new LinkedHashMap<>();

  private final Consumer<SubscriptionPartitionSplit> cursorCommitter;

  public CheckpointCursorCommitter(Consumer<SubscriptionPartitionSplit> cursorCommitter) {
    this.cursorCommitter = cursorCommitter;
  }

  public synchronized void addCheckpoint(
      long checkpointId, Collection<SubscriptionPartitionSplit> checkpoint) {
    checkpoints.put(
        checkpointId,
        ImmutableList.<SubscriptionPartitionSplit>builder()
            .addAll(checkpoint)
            .addAll(finished)
            .build());
  }

  public synchronized void notifyCheckpointComplete(long checkpointId) {
    if (!checkpoints.containsKey(checkpointId)) {
      return;
    }
    // Commit offsets corresponding to this checkpoint.
    List<SubscriptionPartitionSplit> splits = checkpoints.get(checkpointId);
    splits.forEach(cursorCommitter);
    // Prune all checkpoints created before the one we just committed.
    Iterator<Entry<Long, List<SubscriptionPartitionSplit>>> iter =
        checkpoints.entrySet().iterator();
    while (iter.hasNext()) {
      long id = iter.next().getKey();
      iter.remove();
      if (id == checkpointId) {
        break;
      }
    }
  }

  public synchronized void notifySplitFinished(Collection<SubscriptionPartitionSplit> splits) {
    finished.addAll(splits);
  }
}

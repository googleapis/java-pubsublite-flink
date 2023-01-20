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

import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsublite.flink.internal.source.split.SubscriptionPartitionSplit;
import com.google.cloud.pubsublite.internal.CursorClient;
import com.google.cloud.pubsublite.internal.wire.SystemExecutors;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CheckpointCursorCommitter implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(CheckpointCursorCommitter.class);

  @GuardedBy("this")
  private final Set<SubscriptionPartitionSplit> finished = new HashSet<>();

  @GuardedBy("this")
  private final LinkedHashMap<Long, List<SubscriptionPartitionSplit>> checkpoints =
      new LinkedHashMap<>();

  private final CursorClient cursorCommitter;

  public CheckpointCursorCommitter(CursorClient cursorCommitter) {
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

  private void commitCursor(SubscriptionPartitionSplit split) {
    ApiFutures.addCallback(
        cursorCommitter.commitCursor(split.subscriptionPath(), split.partition(), split.start()),
        new ApiFutureCallback<Void>() {
          @Override
          public void onFailure(Throwable throwable) {
            LOG.error("Failed to commit cursor to Pub/Sub Lite ", throwable);
          }

          @Override
          public void onSuccess(Void unused) {}
        },
        SystemExecutors.getAlarmExecutor());
  }

  public synchronized void notifyCheckpointComplete(long checkpointId) {
    if (!checkpoints.containsKey(checkpointId)) {
      return;
    }
    // Commit offsets corresponding to this checkpoint.
    List<SubscriptionPartitionSplit> splits = checkpoints.get(checkpointId);
    splits.forEach(this::commitCursor);
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

  @Override
  public void close() {
    cursorCommitter.close();
  }
}

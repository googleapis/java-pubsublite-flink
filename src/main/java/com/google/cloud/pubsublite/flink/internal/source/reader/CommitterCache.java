/*
 * Copyright 2023 Google LLC
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

import static com.google.cloud.pubsublite.internal.wire.ApiServiceUtils.blockingShutdown;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

import com.google.api.core.ApiService.Listener;
import com.google.api.core.ApiService.State;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.internal.wire.Committer;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import java.util.HashMap;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommitterCache implements CommitterFactory {
  private static final Logger LOG = LoggerFactory.getLogger(CommitterCache.class);
  private final Function<Partition, Committer> underlying;

  @GuardedBy("this")
  private final HashMap<Partition, Committer> partitionCommitters = new HashMap<>();

  public CommitterCache(Function<Partition, Committer> underlying) {
    this.underlying = underlying;
  }

  @Override
  public synchronized Committer getCommitter(Partition partition) {
    return partitionCommitters.computeIfAbsent(
        partition,
        p -> {
          Committer newCommitter = underlying.apply(p);
          newCommitter.addListener(
              new Listener() {
                @Override
                public void failed(State from, Throwable failure) {
                  LOG.info("Committer failed.", failure);
                  remove(p);
                }

                @Override
                public void terminated(State from) {
                  remove(p);
                }
              },
              directExecutor());
          newCommitter.startAsync().awaitRunning();
          return newCommitter;
        });
  }

  @Override
  public synchronized void close() {
    blockingShutdown(partitionCommitters.values());
  }

  private synchronized void remove(Partition partition) {
    partitionCommitters.remove(partition);
  }
}

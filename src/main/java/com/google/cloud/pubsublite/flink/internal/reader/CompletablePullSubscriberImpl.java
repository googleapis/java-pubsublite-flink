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
package com.google.cloud.pubsublite.flink.internal.reader;

import com.google.api.core.ApiFuture;
import com.google.cloud.pubsublite.SequencedMessage;
import com.google.cloud.pubsublite.internal.BlockingPullSubscriber;
import com.google.cloud.pubsublite.internal.CheckedApiException;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import java.util.Optional;

public class CompletablePullSubscriberImpl implements CompletablePullSubscriber {
  private final BlockingPullSubscriber subscriber;
  PartitionFinishedCondition condition;

  @GuardedBy("this")
  boolean finished = false;

  public CompletablePullSubscriberImpl(
      BlockingPullSubscriber subscriber, PartitionFinishedCondition condition) {
    this.subscriber = subscriber;
    this.condition = condition;
  }

  public ApiFuture<Void> onData() {
    return subscriber.onData();
  }

  @Override
  public synchronized Optional<SequencedMessage> messageIfAvailable() throws CheckedApiException {
    if (finished) {
      return Optional.empty();
    }
    Optional<SequencedMessage> m = subscriber.messageIfAvailable();
    if (!m.isPresent()) {
      return Optional.empty();
    }
    switch (condition.partitionFinished(m.get().offset())) {
      case CONTINUE:
        return m;
      case FINISH_AFTER:
        finished = true;
        return m;
      case FINISH_BEFORE:
        finished = true;
        return Optional.empty();
      default:
        // unreachable.
        return Optional.empty();
    }
  }

  @Override
  public synchronized boolean isFinished() {
    return finished;
  }

  @Override
  public void close() {
    subscriber.close();
  }
}

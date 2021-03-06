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

import com.google.cloud.pubsublite.flink.internal.split.SubscriptionPartitionSplit;
import com.google.cloud.pubsublite.internal.BlockingPullSubscriber;
import com.google.cloud.pubsublite.internal.CheckedApiException;

/** A blocking pull subscriber which can terminate based on a condition. */
public interface CompletablePullSubscriber extends BlockingPullSubscriber, AutoCloseable {
  /** Indicate whether the subscriber is finished and will have no more messages. */
  boolean isFinished();

  interface Factory {
    /**
     * Create a subscriber which will subscribe on the partition corresponding to the provided
     * split.
     */
    CompletablePullSubscriber New(SubscriptionPartitionSplit split) throws CheckedApiException;
  }
}

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
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsublite.SequencedMessage;
import com.google.cloud.pubsublite.flink.internal.reader.PartitionFinishedCondition.Result;
import com.google.cloud.pubsublite.flink.internal.split.SubscriptionPartitionSplit;
import com.google.cloud.pubsublite.internal.CheckedApiException;
import java.util.Optional;

// A completable pull subscriber factory which checks to see if the split is already finished and
// if so, creates a completed subscriber. Otherwise it delegates subscriber creation to the
// underlying factory.
public class ConditionallyCompleteSubscriberFactory implements CompletablePullSubscriber.Factory {

  private static class FinishedSubscriber implements CompletablePullSubscriber {
    @Override
    public boolean isFinished() {
      return true;
    }

    @Override
    public ApiFuture<Void> onData() {
      return ApiFutures.immediateFuture(null);
    }

    @Override
    public Optional<SequencedMessage> messageIfAvailable() {
      return Optional.empty();
    }

    @Override
    public void close() {}
  }

  private final CompletablePullSubscriber.Factory subscriberFactory;
  private final PartitionFinishedCondition.Factory conditionFactory;

  public ConditionallyCompleteSubscriberFactory(
      CompletablePullSubscriber.Factory subscriberFactory,
      PartitionFinishedCondition.Factory conditionFactory) {
    this.subscriberFactory = subscriberFactory;
    this.conditionFactory = conditionFactory;
  }

  @Override
  public CompletablePullSubscriber New(SubscriptionPartitionSplit split)
      throws CheckedApiException {
    PartitionFinishedCondition condition =
        conditionFactory.New(split.subscriptionPath(), split.partition());
    if (condition.partitionFinished(split.start()) != Result.CONTINUE) {
      return new FinishedSubscriber();
    }
    return subscriberFactory.New(split);
  }
}

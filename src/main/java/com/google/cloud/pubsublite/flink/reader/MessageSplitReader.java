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

import com.google.api.core.ApiFuture;
import com.google.api.core.SettableApiFuture;
import com.google.cloud.pubsublite.SequencedMessage;
import com.google.cloud.pubsublite.flink.split.SubscriptionPartitionSplit;
import com.google.cloud.pubsublite.internal.BlockingPullSubscriber;
import com.google.cloud.pubsublite.internal.CheckedApiException;
import com.google.cloud.pubsublite.internal.ExtractStatus;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageSplitReader
    implements SplitReader<SequencedMessage, SubscriptionPartitionSplit> {
  private static final int FETCH_TIMEOUT_MS = 1000;
  private static final Logger LOG = LoggerFactory.getLogger(MessageSplitReader.class);

  private static class MultiplexedSubscriber {
    final Map<String, BlockingPullSubscriber> subscribers;

    MultiplexedSubscriber(Map<String, BlockingPullSubscriber> subscribers) {
      this.subscribers = subscribers;
    }

    Multimap<String, SequencedMessage> getMessages() throws CheckedApiException {
      HashMultimap<String, SequencedMessage> messages = HashMultimap.create();
      for (Map.Entry<String, BlockingPullSubscriber> entry : subscribers.entrySet()) {
        String splitId = entry.getKey();
        BlockingPullSubscriber sub = entry.getValue();
        sub.messageIfAvailable().ifPresent(m -> messages.put(splitId, m));
      }
      return messages;
    }

    ApiFuture<Void> onData() {
      // Note that this doesn't propagate failure of the child futures
      SettableApiFuture<Void> future = SettableApiFuture.create();
      subscribers
          .values()
          .forEach(
              s -> s.onData().addListener(() -> future.set(null), MoreExecutors.directExecutor()));
      return future;
    }
  }

  private final CompletablePullSubscriber.Factory factory;

  @GuardedBy("this")
  private final Map<SubscriptionPartitionSplit, CompletablePullSubscriber> splitStates =
      new HashMap<>();

  @GuardedBy("this")
  private List<SubscriptionPartitionSplit> newSplits = new ArrayList<>();

  @GuardedBy("this")
  private MultiplexedSubscriber subscriber = new MultiplexedSubscriber(new HashMap<>());

  public MessageSplitReader(CompletablePullSubscriber.Factory factory) {
    this.factory = factory;
  }

  @Override
  public RecordsBySplits<SequencedMessage> fetch() throws IOException {
    handleNewSplits();
    try {
      currentSubscriber().onData().get(FETCH_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } catch (ExecutionException | InterruptedException e) {
      throw new IOException(ExtractStatus.toCanonical(e));
    } catch (TimeoutException ignored) {
    }
    RecordsBySplits.Builder<SequencedMessage> builder = new RecordsBySplits.Builder<>();
    try {
      currentSubscriber().getMessages().asMap().forEach(builder::addAll);
    } catch (CheckedApiException e) {
      throw new IOException(e.underlying);
    }
    builder.addFinishedSplits(handleFinishedSplits());
    return builder.build();
  }

  private synchronized MultiplexedSubscriber currentSubscriber() {
    return subscriber;
  }

  private synchronized Map<SubscriptionPartitionSplit, CompletablePullSubscriber>
      removeFinishedSplits() {
    Map<SubscriptionPartitionSplit, CompletablePullSubscriber> finished =
        splitStates.entrySet().stream()
            .filter(entry -> entry.getValue().isFinished())
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    finished.keySet().forEach(splitStates::remove);
    subscriber =
        new MultiplexedSubscriber(
            splitStates.entrySet().stream()
                .collect(Collectors.toMap(e -> e.getKey().splitId(), Entry::getValue)));
    return finished;
  }

  private List<String> handleFinishedSplits() {
    Map<SubscriptionPartitionSplit, CompletablePullSubscriber> finished = removeFinishedSplits();
    Exception e = closeAll(finished.values());
    if (e != null) {
      LOG.error("Exception while closing a subscriber", e);
    }
    return finished.keySet().stream()
        .map(SubscriptionPartitionSplit::splitId)
        .collect(Collectors.toList());
  }

  private void handleNewSplits() throws IOException {
    List<SubscriptionPartitionSplit> toStart;
    synchronized (this) {
      if (newSplits.isEmpty()) {
        return;
      }
      toStart = newSplits;
      newSplits = new ArrayList<>();
    }
    Map<SubscriptionPartitionSplit, CompletablePullSubscriber> newSubscribers = new HashMap<>();
    for (SubscriptionPartitionSplit newSplit : toStart) {
      try {
        newSubscribers.put(newSplit, factory.New(newSplit));
      } catch (CheckedApiException e) {
        throw new IOException(ExtractStatus.toCanonical(e).underlying);
      }
    }
    synchronized (this) {
      splitStates.putAll(newSubscribers);
      subscriber =
          new MultiplexedSubscriber(
              splitStates.entrySet().stream()
                  .collect(Collectors.toMap(e -> e.getKey().splitId(), Entry::getValue)));
    }
  }

  private Exception closeAll(Collection<CompletablePullSubscriber> subscribers) {
    Exception exception = null;
    for (CompletablePullSubscriber sub : subscribers) {
      try {
        sub.close();
      } catch (Exception t) {
        exception = t;
      }
    }
    return exception;
  }

  @Override
  public synchronized void handleSplitsChanges(
      SplitsChange<SubscriptionPartitionSplit> splitsChange) {
    LOG.info("received a splits changed event {}", splitsChange);
    if (!(splitsChange instanceof SplitsAddition)) {
      throw new IllegalArgumentException("Unexpected split event " + splitsChange);
    }
    // Since this method isn't supposed to block or except, we defer actually initializing the
    // splits to later.
    newSplits.addAll(splitsChange.splits());
  }

  @Override
  public void wakeUp() {
    // We never block for more than 1000 ms
  }

  @Override
  public synchronized void close() throws Exception {
    Exception t = closeAll(splitStates.values());
    if (t != null) {
      throw t;
    }
  }
}

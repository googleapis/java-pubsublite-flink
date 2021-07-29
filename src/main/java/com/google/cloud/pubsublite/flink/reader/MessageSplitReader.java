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
import com.google.cloud.pubsublite.flink.reader.CompletablePullSubscriber.Factory;
import com.google.cloud.pubsublite.flink.split.SubscriptionPartitionSplit;
import com.google.cloud.pubsublite.internal.BlockingPullSubscriber;
import com.google.cloud.pubsublite.internal.CheckedApiException;
import com.google.cloud.pubsublite.internal.ExtractStatus;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
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

  final CompletablePullSubscriber.Factory factory;
  final Map<String, CompletablePullSubscriber> subscribers;

  public MessageSplitReader(Factory factory) {
    this.factory = factory;
    this.subscribers = new HashMap<>();
  }

  private Multimap<String, SequencedMessage> getMessages() throws CheckedApiException {
    // TODO: Consider pulling more than one message to avoid creating a future per message.
    ImmutableListMultimap.Builder<String, SequencedMessage> messages =
        ImmutableListMultimap.builder();
    for (Map.Entry<String, CompletablePullSubscriber> entry : subscribers.entrySet()) {
      String splitId = entry.getKey();
      BlockingPullSubscriber sub = entry.getValue();
      sub.messageIfAvailable().ifPresent(m -> messages.put(splitId, m));
    }
    return messages.build();
  }

  private ApiFuture<Void> onData() {
    // Note that this doesn't propagate failure of the child futures
    SettableApiFuture<Void> future = SettableApiFuture.create();
    subscribers
        .values()
        .forEach(
            s -> s.onData().addListener(() -> future.set(null), MoreExecutors.directExecutor()));
    return future;
  }

  Collection<String> removeFinishedSubscribers() {
    Map<String, CompletablePullSubscriber> finished =
        subscribers.entrySet().stream()
            .filter(entry -> entry.getValue().isFinished())
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    finished.keySet().forEach(subscribers::remove);
    try {
      closeAll(finished.values());
    } catch (Exception e) {
      LOG.error("Exception while trying to close subscribers", e);
    }
    return finished.keySet();
  }

  @Override
  public RecordsBySplits<SequencedMessage> fetch() throws IOException {
    try {
      onData().get(FETCH_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } catch (ExecutionException | InterruptedException e) {
      throw new IOException(ExtractStatus.toCanonical(e));
    } catch (TimeoutException ignored) {
    }
    RecordsBySplits.Builder<SequencedMessage> builder = new RecordsBySplits.Builder<>();
    try {
      getMessages().asMap().forEach(builder::addAll);
    } catch (CheckedApiException e) {
      throw new IOException(e.underlying);
    }
    builder.addFinishedSplits(removeFinishedSubscribers());
    return builder.build();
  }

  @Override
  public synchronized void handleSplitsChanges(
      SplitsChange<SubscriptionPartitionSplit> splitsChange) {
    LOG.info("received a splits changed event {}", splitsChange);
    if (!(splitsChange instanceof SplitsAddition)) {
      throw new IllegalArgumentException("Unexpected split event " + splitsChange);
    }
    try {
      for (SubscriptionPartitionSplit newSplit : splitsChange.splits()) {
        if (!subscribers.containsKey(newSplit.splitId())) {
          subscribers.put(newSplit.splitId(), factory.New(newSplit));
          LOG.info("Adding split {} to fetcher", newSplit);
        } else {
          LOG.error("Adding split {} which was already added to the fetcher", newSplit);
        }
      }
    } catch (CheckedApiException e) {
      throw e.underlying;
    }
  }

  @Override
  public void wakeUp() {
    // We never block for more than 1000 ms
  }

  private void closeAll(Collection<CompletablePullSubscriber> subscribers) throws Exception {
    Exception exception = null;
    for (CompletablePullSubscriber sub : subscribers) {
      try {
        sub.close();
      } catch (Exception t) {
        exception = t;
      }
    }
    if (exception != null) {
      throw exception;
    }
  }

  @Override
  public void close() throws Exception {
    closeAll(subscribers.values());
  }
}

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
import java.io.IOException;
import java.util.*;
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
      SettableApiFuture<Void> future = SettableApiFuture.create();
      subscribers
          .values()
          .forEach(
              s -> s.onData().addListener(() -> future.set(null), MoreExecutors.directExecutor()));
      return future;
    }
  }

  private final CompletablePullSubscriber.Factory factory;

  private final List<SubscriptionPartitionSplit> newSplits = new ArrayList<>();
  private final Map<SubscriptionPartitionSplit, CompletablePullSubscriber> splitStates =
      new HashMap<>();
  private MultiplexedSubscriber subscriber = new MultiplexedSubscriber(new HashMap<>());

  public MessageSplitReader(CompletablePullSubscriber.Factory factory) {
    this.factory = factory;
  }

  @Override
  public RecordsBySplits<SequencedMessage> fetch() throws IOException {
    addNewSplits();
    try {
      subscriber.onData().get(1000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException(ExtractStatus.toCanonical(e).underlying);
    } catch (TimeoutException ignored) {
    }
    RecordsBySplits.Builder<SequencedMessage> builder = new RecordsBySplits.Builder<>();
    try {
      Map<String, Collection<SequencedMessage>> messages = subscriber.getMessages().asMap();
      System.out.println(messages);
      messages.forEach(builder::addAll);
    } catch (CheckedApiException e) {
      throw new IOException(e);
    }
    builder.addFinishedSplits(removedFinishedSplits());
    return builder.build();
  }

  private void recreateSubscriber() {
    Map<String, BlockingPullSubscriber> subscriberMap = new HashMap<>();
    splitStates.forEach((split, sub) -> subscriberMap.put(split.splitId(), sub));
    subscriber = new MultiplexedSubscriber(subscriberMap);
  }

  private List<String> removedFinishedSplits() {
    List<SubscriptionPartitionSplit> toRemove =
        splitStates.entrySet().stream()
            .filter(entry -> entry.getValue().isFinished())
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());
    List<CompletablePullSubscriber> toClose =
        toRemove.stream().map(splitStates::remove).collect(Collectors.toList());
    // Log this;
    Exception e = closeAll(toClose);
    if (e != null) {
      LOG.error("Exception while closing a subscriber", e);
    }
    if (!toRemove.isEmpty()) {
      recreateSubscriber();
    }
    return toRemove.stream().map(SubscriptionPartitionSplit::splitId).collect(Collectors.toList());
  }

  private void addNewSplits() throws IOException {
    for (SubscriptionPartitionSplit newSplit : newSplits) {
      try {
        splitStates.put(newSplit, factory.New(newSplit));
      } catch (CheckedApiException e) {
        throw new IOException(ExtractStatus.toCanonical(e).underlying);
      }
    }
    if (!newSplits.isEmpty()) {
      recreateSubscriber();
    }
    newSplits.clear();
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
    subscribers.clear();
    return exception;
  }

  @Override
  public void handleSplitsChanges(SplitsChange<SubscriptionPartitionSplit> splitsChange) {
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
  public void close() throws Exception {
    Exception t = closeAll(splitStates.values());
    if (t != null) {
      throw t;
    }
  }
}

/*
 * Copyright 2020 Google LLC
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

package com.google.cloud.pubsublite.flink.internal.sink;

import com.google.api.core.ApiService.Listener;
import com.google.api.core.ApiService.State;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsublite.MessageMetadata;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.flink.PubsubLiteSinkSettings;
import com.google.cloud.pubsublite.internal.Publisher;
import com.google.cloud.pubsublite.internal.wire.SystemExecutors;
import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A map of working publishers by PublisherOptions. */
public class PublisherCache implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(PublisherCache.class);

  interface PublisherFactory {
    Publisher<MessageMetadata> New(PubsubLiteSinkSettings<?> options);
  }

  @GuardedBy("this")
  private final HashMap<TopicPath, Publisher<MessageMetadata>> livePublishers = new HashMap<>();

  private final PublisherFactory factory;

  public PublisherCache(PublisherFactory factory) {
    this.factory = factory;
  }

  private synchronized void evict(TopicPath topic) {
    livePublishers.remove(topic);
  }

  public synchronized Publisher<MessageMetadata> get(PubsubLiteSinkSettings<?> options)
      throws ApiException {
    LOG.info("Requesting a publisher for options {}", options);
    Publisher<MessageMetadata> publisher = livePublishers.get(options.topicPath());
    if (publisher != null) {
      return publisher;
    }
    publisher = factory.New(options);
    publisher.addListener(
        new Listener() {
          @Override
          public void failed(State s, Throwable t) {
            LOG.error("Publisher for options {} failed with exception", options, t);
            evict(options.topicPath());
          }
        },
        SystemExecutors.getAlarmExecutor());
    publisher.startAsync();
    LOG.info("Successfully started publisher for options {}", options);
    livePublishers.put(options.topicPath(), publisher);
    return publisher;
  }

  @VisibleForTesting
  public synchronized void set(TopicPath topic, Publisher<MessageMetadata> toCache) {
    livePublishers.put(topic, toCache);
    toCache.addListener(
        new Listener() {
          @Override
          public void failed(State s, Throwable t) {
            LOG.error("Publisher for topic {} failed with exception", topic, t);
            evict(topic);
          }
        },
        SystemExecutors.getAlarmExecutor());
  }

  @Override
  public synchronized void close() {
    livePublishers.forEach(((options, publisher) -> publisher.stopAsync()));
    livePublishers.clear();
  }
}

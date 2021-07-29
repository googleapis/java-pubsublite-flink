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

package com.google.cloud.pubsublite.flink.sink;

import com.google.api.core.ApiService.Listener;
import com.google.api.core.ApiService.State;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsublite.MessageMetadata;
import com.google.cloud.pubsublite.internal.Publisher;
import com.google.cloud.pubsublite.internal.wire.SystemExecutors;
import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import java.util.HashMap;

/** A map of working publishers by PublisherOptions. */
public class PublisherCache<T> implements AutoCloseable {
  interface PublisherFactory<T> {
    Publisher<MessageMetadata> New(T options);
  }

  @GuardedBy("this")
  private final HashMap<T, Publisher<MessageMetadata>> livePublishers = new HashMap<>();

  private final PublisherFactory<T> factory;

  public PublisherCache(PublisherFactory<T> factory) {
    this.factory = factory;
  }

  private synchronized void evict(T options) {
    livePublishers.remove(options);
  }

  public synchronized Publisher<MessageMetadata> get(T options) throws ApiException {
    Publisher<MessageMetadata> publisher = livePublishers.get(options);
    if (publisher != null) {
      return publisher;
    }
    publisher = factory.New(options);
    publisher.addListener(
        new Listener() {
          @Override
          public void failed(State s, Throwable t) {
            evict(options);
          }
        },
        SystemExecutors.getAlarmExecutor());
    publisher.startAsync().awaitRunning();
    livePublishers.put(options, publisher);
    return publisher;
  }

  @VisibleForTesting
  public synchronized void set(T options, Publisher<MessageMetadata> toCache) {
    livePublishers.put(options, toCache);
    toCache.addListener(
        new Listener() {
          @Override
          public void failed(State s, Throwable t) {
            evict(options);
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

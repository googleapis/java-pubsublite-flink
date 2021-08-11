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
package com.google.cloud.pubsublite.flink.internal.sink;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsublite.Message;
import com.google.cloud.pubsublite.MessageMetadata;
import com.google.cloud.pubsublite.internal.CheckedApiException;
import com.google.cloud.pubsublite.internal.ExtractStatus;
import com.google.cloud.pubsublite.internal.Publisher;
import com.google.cloud.pubsublite.internal.wire.SystemExecutors;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessagePublisher implements BulkWaitPublisher<Message> {
  private static final Logger LOG = LoggerFactory.getLogger(BulkWaitPublisher.class);
  private final Publisher<MessageMetadata> publisher;
  private final List<ApiFuture<MessageMetadata>> publishes;

  private static final long maxBytesOutstanding = 10*1024*1024;
  @GuardedBy("this")
  private long bytesOutstanding = 0;

  public MessagePublisher(Publisher<MessageMetadata> publisher) {
    this.publisher = publisher;
    this.publishes = new ArrayList<>();
  }

  private synchronized void addOutstanding(long bytes) {
    bytesOutstanding += bytes;
  }
  private synchronized void removeOutstanding(long bytes) {
    bytesOutstanding -= bytes;
  }

  private synchronized void waitForCapacity() {
    if (bytesOutstanding > maxBytesOutstanding) {
      LOG.info("Publisher outstanding byte limit exceeded. Waiting for outstanding publishes");
      try {
        // Pause the publisher until all outstanding publishes finish.
        ApiFutures.allAsList(publishes).get();
      } catch (Throwable t) {
        // ignored. Errors will be thrown by waitUntilNoOutstandingPublishes()
      }
      LOG.info("Outstanding publishes completed, continuing");
    }

  }

  @Override
  public void publish(Message message) {
    long size = message.toProto().getSerializedSize();
    waitForCapacity();
    addOutstanding(size);
    ApiFuture<MessageMetadata> future = publisher.publish(message);
    future.addListener(() -> removeOutstanding(size), SystemExecutors.getAlarmExecutor());
    publishes.add(future);
  }

  @Override
  public void waitUntilNoOutstandingPublishes() throws CheckedApiException {
    try {
      ApiFutures.allAsList(publishes).get();
      publishes.clear();
    } catch (Exception e) {
      throw ExtractStatus.toCanonical(e);
    }
  }
}

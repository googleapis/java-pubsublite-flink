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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessagePublisher implements BulkWaitPublisher<Message> {
  private static final Logger LOG = LoggerFactory.getLogger(MessagePublisher.class);
  private final Publisher<MessageMetadata> publisher;
  private final List<ApiFuture<MessageMetadata>> publishes;

  private final int maxBytesOutstanding;
  private final Semaphore bytesOutstanding;

  public MessagePublisher(Publisher<MessageMetadata> publisher, int maxBytesOutstanding) {
    this.publisher = publisher;
    this.publishes = new ArrayList<>();
    this.maxBytesOutstanding = maxBytesOutstanding;
    this.bytesOutstanding = new Semaphore(maxBytesOutstanding);
  }

  private int getAccountedSize(Message message) {
    long size = message.toProto().getSerializedSize();
    if (size > maxBytesOutstanding) {
      return maxBytesOutstanding;
    }
    return Math.toIntExact(size);
  }

  @Override
  public void publish(Message message) throws InterruptedException {
    final int size = getAccountedSize(message);
    if (!bytesOutstanding.tryAcquire()) {
      LOG.warn(
          "Publisher flow controlled due to too many bytes (>{}) outstanding", maxBytesOutstanding);
      bytesOutstanding.acquire(size);
    }
    ApiFuture<MessageMetadata> future = publisher.publish(message);
    future.addListener(() -> bytesOutstanding.release(size), SystemExecutors.getAlarmExecutor());
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

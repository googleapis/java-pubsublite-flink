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
package com.google.cloud.pubsublite.flink.sink;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.core.ApiService.Listener;
import com.google.api.core.ApiService.State;
import com.google.cloud.pubsublite.Message;
import com.google.cloud.pubsublite.MessageMetadata;
import com.google.cloud.pubsublite.internal.CheckedApiException;
import com.google.cloud.pubsublite.internal.ExtractStatus;
import com.google.cloud.pubsublite.internal.Publisher;
import com.google.cloud.pubsublite.internal.wire.SystemExecutors;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import java.util.ArrayDeque;
import java.util.Deque;

public class MessagePublisher implements AtLeastOncePublisher<Message> {
  private static class MessageTracker {

    @GuardedBy("this")
    private final Deque<CheckedApiException> errors = new ArrayDeque<>();

    @GuardedBy("this")
    private int counter = 0;

    private static CheckedApiException combine(Deque<CheckedApiException> errors) {
      CheckedApiException canonical = errors.pop();
      while (!errors.isEmpty()) {
        canonical.addSuppressed(errors.pop());
      }
      return canonical;
    }

    public synchronized void failTracker(Exception e) {
      errors.add(ExtractStatus.toCanonical(e));
      notify();
    }

    public synchronized ApiFutureCallback<MessageMetadata> addOutstanding()
        throws CheckedApiException {
      if (!errors.isEmpty()) {
        throw combine(errors);
      }
      counter++;
      return new ApiFutureCallback<MessageMetadata>() {
        @Override
        public void onFailure(Throwable throwable) {
          synchronized (MessageTracker.this) {
            errors.add(ExtractStatus.toCanonical(throwable));
            counter--;
            MessageTracker.this.notify();
          }
        }

        @Override
        public void onSuccess(MessageMetadata unused) {
          synchronized (MessageTracker.this) {
            counter--;
            MessageTracker.this.notify();
          }
        }
      };
    }

    public synchronized void waitUntilNoneOutstanding()
        throws CheckedApiException, InterruptedException {
      while (counter > 0 && errors.isEmpty()) {
        wait();
      }
      if (!errors.isEmpty()) {
        throw combine(errors);
      }
    }
  }

  private final Publisher<MessageMetadata> publisher;
  private final MessageTracker tracker;

  public MessagePublisher(Publisher<MessageMetadata> publisher) {
    this.tracker = new MessageTracker();
    this.publisher = publisher;
    this.publisher.addListener(
        new Listener() {
          @Override
          public void failed(State from, Throwable failure) {
            tracker.failTracker(ExtractStatus.toCanonical(failure).underlying);
          }
        },
        SystemExecutors.getAlarmExecutor());
    this.publisher.startAsync();
    this.publisher.awaitRunning();
  }

  @Override
  public void publish(Message message) throws CheckedApiException {
    ApiFutureCallback<MessageMetadata> done = tracker.addOutstanding();
    ApiFuture<MessageMetadata> future = publisher.publish(message);
    ApiFutures.addCallback(future, done, SystemExecutors.getAlarmExecutor());
  }

  @Override
  public void waitUntilNoOutstandingPublishes() throws CheckedApiException, InterruptedException {
    tracker.waitUntilNoneOutstanding();
  }
}

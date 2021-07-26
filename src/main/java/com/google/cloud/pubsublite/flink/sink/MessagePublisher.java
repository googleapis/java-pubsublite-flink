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
import com.google.common.util.concurrent.MoreExecutors;
import com.google.errorprone.annotations.concurrent.GuardedBy;

public class MessagePublisher implements AtLeastOncePublisher<Message> {
  private static class MessageTracker {

    @GuardedBy("this")
    private CheckedApiException error = null;

    @GuardedBy("this")
    private int counter = 0;

    public synchronized void failTracker(Exception e) {
      error = ExtractStatus.toCanonical(e);
      notify();
    }

    public synchronized ApiFutureCallback<MessageMetadata> addOutstanding()
        throws CheckedApiException {
      if (error != null) {
        throw error;
      }
      counter++;
      return new ApiFutureCallback<MessageMetadata>() {
        @Override
        public void onFailure(Throwable throwable) {
          synchronized (MessageTracker.this) {
            error = ExtractStatus.toCanonical(throwable);
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
      while (counter > 0 && error == null) {
        wait();
      }
      if (error != null) {
        throw error;
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
        MoreExecutors.directExecutor());
    this.publisher.startAsync();
    this.publisher.awaitRunning();
  }

  @Override
  public void publish(Message message) throws CheckedApiException {
    ApiFutureCallback<MessageMetadata> done = tracker.addOutstanding();
    ApiFuture<MessageMetadata> future = publisher.publish(message);
    ApiFutures.addCallback(future, done, MoreExecutors.directExecutor());
  }

  @Override
  public void waitUntilNoOutstandingPublishes() throws CheckedApiException, InterruptedException {
    tracker.waitUntilNoneOutstanding();
  }
}

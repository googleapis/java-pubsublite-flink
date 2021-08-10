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
import com.google.api.core.ApiService.State;
import com.google.cloud.pubsublite.Message;
import com.google.cloud.pubsublite.MessageMetadata;
import com.google.cloud.pubsublite.internal.CheckedApiException;
import com.google.cloud.pubsublite.internal.ExtractStatus;
import com.google.cloud.pubsublite.internal.Publisher;
import java.util.ArrayList;
import java.util.List;

public class MessagePublisher implements BulkWaitPublisher<Message> {
  private final Publisher<MessageMetadata> publisher;
  private final List<ApiFuture<MessageMetadata>> publishes;

  public MessagePublisher(Publisher<MessageMetadata> publisher) {
    this.publisher = publisher;
    this.publishes = new ArrayList<>();
  }

  @Override
  public void publish(Message message) {
    try {
      publishes.add(publisher.publish(message));
    } catch (IllegalStateException e) {
      if(publisher.state() == State.FAILED) {
        throw new IllegalStateException("Publisher failed with cause: " + publisher.failureCause());
      } else {
        throw new IllegalStateException("Cannot publish, publisher in state " + publisher.state() + publisher.toString() + publisher.failureCause());
      }
    }
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

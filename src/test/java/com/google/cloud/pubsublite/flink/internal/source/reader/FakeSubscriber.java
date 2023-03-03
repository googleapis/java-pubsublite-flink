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
package com.google.cloud.pubsublite.flink.internal.source.reader;


import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.core.SettableApiFuture;
import com.google.cloud.pubsublite.internal.BlockingPullSubscriber;
import com.google.cloud.pubsublite.proto.SequencedMessage;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Optional;
import java.util.Queue;

public class FakeSubscriber implements BlockingPullSubscriber {
  private final Queue<Optional<SequencedMessage>> messages;

  public FakeSubscriber(Collection<Optional<SequencedMessage>> messages) {
    this.messages = new ArrayDeque<>(messages);
  }

  @Override
  public ApiFuture<Void> onData() {
    if (messages.isEmpty()) {
      return SettableApiFuture.create();
    }
    return ApiFutures.immediateFuture(null);
  }

  @Override
  public Optional<SequencedMessage> messageIfAvailable() {
    if (messages.isEmpty()) {
      return Optional.empty();
    }
    return messages.poll();
  }

  @Override
  public void close() {}
}

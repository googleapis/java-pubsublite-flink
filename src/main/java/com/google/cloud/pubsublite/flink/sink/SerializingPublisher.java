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

import com.google.cloud.Tuple;
import com.google.cloud.pubsublite.Message;
import com.google.cloud.pubsublite.flink.PubsubLiteSerializationSchema;
import com.google.cloud.pubsublite.internal.CheckedApiException;
import java.time.Instant;

public class SerializingPublisher<T> implements AtLeastOncePublisher<Tuple<T, Instant>> {
  private final AtLeastOncePublisher<Message> inner;
  private final PubsubLiteSerializationSchema<T> schema;

  public SerializingPublisher(
      AtLeastOncePublisher<Message> inner, PubsubLiteSerializationSchema<T> schema) {
    this.inner = inner;
    this.schema = schema;
  }

  @Override
  public void publish(Tuple<T, Instant> message) {
    Message publish;
    publish = schema.serialize(message.x(), message.y());
    inner.publish(publish);
  }

  @Override
  public void waitUntilNoOutstandingPublishes() throws CheckedApiException {
    inner.waitUntilNoOutstandingPublishes();
  }
}

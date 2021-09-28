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
package com.google.cloud.pubsublite.flink;

import com.google.auto.value.AutoValue;
import com.google.cloud.pubsublite.Message;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.flink.internal.sink.PublisherOptions;
import java.io.Serializable;

@AutoValue
public abstract class PubsubLiteSinkSettings<InputT> implements Serializable {
  public static final int DEFAULT_MAX_BYTES_OUTSTANDING = 100 * 1024 * 1024;
  // Create a builder which will accept messages of type InputT and serialize them using the
  // provided serialization schema.
  public static <InputT> Builder<InputT> builder(PubsubLiteSerializationSchema<InputT> schema) {
    return new AutoValue_PubsubLiteSinkSettings.Builder<InputT>()
        .setMaxBytesOutstanding(DEFAULT_MAX_BYTES_OUTSTANDING)
        .setSerializationSchema(schema);
  }

  // Create a sink which will accept already serialized pubsub messages/
  public static Builder<Message> messagesBuilder() {
    return builder(PubsubLiteSerializationSchema.messageSchema());
  }

  // Required. The path of the topic to publish messages to.
  public abstract TopicPath topicPath();

  // Optional. The maximum number of bytes a sink task may have outstanding.
  public abstract Integer maxBytesOutstanding();

  // Internal.
  abstract PubsubLiteSerializationSchema<InputT> serializationSchema();

  PublisherOptions getPublisherConfig() {
    return PublisherOptions.create(topicPath());
  }

  @AutoValue.Builder
  public abstract static class Builder<InputT> {
    // Required.
    public abstract Builder<InputT> setTopicPath(TopicPath value);

    // Optional.
    public abstract Builder<InputT> setMaxBytesOutstanding(Integer value);

    // Internal.
    abstract Builder<InputT> setSerializationSchema(PubsubLiteSerializationSchema<InputT> value);

    public abstract PubsubLiteSinkSettings<InputT> build();
  }
}

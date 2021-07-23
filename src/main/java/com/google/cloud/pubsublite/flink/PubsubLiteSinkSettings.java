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
import com.google.cloud.pubsublite.flink.sink.PublisherOptions;
import java.io.Serializable;

@AutoValue
public abstract class PubsubLiteSinkSettings<InputT> implements Serializable {
  public static <InputT> PubsubLiteSinkSettings.Builder<InputT> builder(
      PubsubLiteSerializationSchema<InputT> schema) {
    return new AutoValue_PubsubLiteSinkSettings.Builder<InputT>().setSerializationSchema(schema);
  }

  public static PubsubLiteSinkSettings.Builder<Message> messagesBuilder() {
    return builder(PubsubLiteSerializationSchema.messageSchema());
  }

  // Required
  public abstract TopicPath topicPath();

  abstract PubsubLiteSerializationSchema<InputT> serializationSchema();

  PublisherOptions getPublisherConfig() {
    return PublisherOptions.create(topicPath());
  }

  @AutoValue.Builder
  abstract static class Builder<InputT> {
    public abstract Builder<InputT> setTopicPath(TopicPath value);

    abstract Builder<InputT> setSerializationSchema(PubsubLiteSerializationSchema<InputT> value);

    public abstract PubsubLiteSinkSettings<InputT> build();
  }
}

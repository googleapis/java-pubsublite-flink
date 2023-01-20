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
import com.google.cloud.pubsublite.SequencedMessage;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.cloudpubsub.FlowControlSettings;
import java.io.Serializable;

@AutoValue
public abstract class PubsubLiteSourceSettings<OutputT> implements Serializable {
  private static final long serialVersionUID = 3206181560865850636L;

  public static <OutputT> Builder<OutputT> builder(
      PubsubLiteDeserializationSchema<OutputT> schema) {
    return new AutoValue_PubsubLiteSourceSettings.Builder<OutputT>()
        .setDeserializationSchema(schema)
        .setTimestampSelector(MessageTimestampExtractor.publishTimeExtractor());
  }

  public static Builder<SequencedMessage> messagesBuilder() {
    return builder(PubsubLiteDeserializationSchema.sequencedMessageSchema());
  }

  // Required
  public abstract SubscriptionPath subscriptionPath();

  // Required
  public abstract FlowControlSettings flowControlSettings();

  // Optional
  public abstract MessageTimestampExtractor timestampSelector();

  // Internal
  public abstract PubsubLiteDeserializationSchema<OutputT> deserializationSchema();

  @AutoValue.Builder
  public abstract static class Builder<OutputT> {
    // Required
    public abstract Builder<OutputT> setSubscriptionPath(SubscriptionPath path);

    // Required
    public abstract Builder<OutputT> setFlowControlSettings(FlowControlSettings settings);

    // Optional
    public abstract Builder<OutputT> setTimestampSelector(MessageTimestampExtractor value);

    abstract Builder<OutputT> setDeserializationSchema(
        PubsubLiteDeserializationSchema<OutputT> schema);

    public abstract PubsubLiteSourceSettings<OutputT> build();
  }
}

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

import com.google.cloud.pubsublite.SequencedMessage;
import java.io.Serializable;
import javax.annotation.Nullable;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema.InitializationContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public interface PubsubLiteDeserializationSchema<T> extends Serializable {

  static <T> PubsubLiteDeserializationSchema<T> dataOnly(DeserializationSchema<T> schema) {
    return new PubsubLiteDeserializationSchema<T>() {
      @Override
      public void open(DeserializationSchema.InitializationContext context) throws Exception {
        schema.open(context);
      }

      @Override
      public T deserialize(SequencedMessage message) throws Exception {
        return schema.deserialize(message.message().data().toByteArray());
      }

      @Override
      public TypeInformation<T> getProducedType() {
        return schema.getProducedType();
      }
    };
  }

  static PubsubLiteDeserializationSchema<SequencedMessage> sequencedMessageSchema() {
    return new PubsubLiteDeserializationSchema<SequencedMessage>() {
      @Override
      public void open(InitializationContext context) {}

      @Override
      public SequencedMessage deserialize(com.google.cloud.pubsublite.SequencedMessage message) {
        return message;
      }

      @Override
      public TypeInformation<SequencedMessage> getProducedType() {
        return TypeInformation.of(SequencedMessage.class);
      }
    };
  }

  void open(DeserializationSchema.InitializationContext context) throws Exception;

  /**
   * Deserialize a Pub/Sub Lite message
   *
   * <p>If a message cannot be deserialized, the schema can either thrown a exception which will
   * fail the source, or it can return null in which case the source will skip the message and
   * proceed.
   *
   * @param message The pub/sub lite message
   * @return The deserialized message as an object (null if the message cannot be deserialized).
   */
  @Nullable
  T deserialize(SequencedMessage message) throws Exception;

  TypeInformation<T> getProducedType();
}

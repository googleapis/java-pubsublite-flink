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

import com.google.cloud.Timestamp;
import com.google.cloud.pubsublite.Message;
import com.google.protobuf.ByteString;
import java.io.Serializable;
import java.sql.Date;
import java.time.Instant;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema.InitializationContext;

public interface PubsubLiteSerializationSchema<T> extends Serializable {
  static <T> PubsubLiteSerializationSchema<T> dataOnly(SerializationSchema<T> schema) {
    return new PubsubLiteSerializationSchema<T>() {

      @Override
      public void open(SerializationSchema.InitializationContext context) throws Exception {
        schema.open(context);
      }

      @Override
      public Message serialize(T value, Instant timestamp) {
        return Message.builder()
            .setData(ByteString.copyFrom(schema.serialize(value)))
            .setEventTime(Timestamp.of(Date.from(timestamp)).toProto())
            .build();
      }
    };
  }

  static PubsubLiteSerializationSchema<Message> messageSchema() {
    return new PubsubLiteSerializationSchema<Message>() {
      @Override
      public void open(InitializationContext context) {}

      @Override
      public Message serialize(Message value, Instant timestamp) {
        return value;
      }
    };
  }

  void open(SerializationSchema.InitializationContext context) throws Exception;

  Message serialize(T value, Instant timestamp);
}

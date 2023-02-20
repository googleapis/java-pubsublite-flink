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
import com.google.cloud.pubsublite.proto.SequencedMessage;
import java.io.Serializable;
import java.time.Instant;

public interface MessageTimestampExtractor extends Serializable {
  static MessageTimestampExtractor publishTimeExtractor() {
    return m -> Timestamp.fromProto(m.getPublishTime()).toDate().toInstant();
  }

  static MessageTimestampExtractor eventTimeExtractor() {
    return m -> {
      if (m.getMessage().hasEventTime()) {
        return Timestamp.fromProto(m.getMessage().getEventTime()).toDate().toInstant();
      }
      return Timestamp.fromProto(m.getPublishTime()).toDate().toInstant();
    };
  }

  Instant timestamp(SequencedMessage m);
}

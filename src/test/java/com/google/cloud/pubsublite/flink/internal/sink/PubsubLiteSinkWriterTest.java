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

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.pubsublite.flink.PubsubLiteSerializationSchema;
import com.google.cloud.pubsublite.proto.PubSubMessage;
import com.google.protobuf.ByteString;
import java.time.Instant;
import org.apache.flink.api.connector.sink2.SinkWriter.Context;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PubsubLiteSinkWriterTest {
  @Mock BulkWaitPublisher mockPublisher;
  @Mock PubsubLiteSerializationSchema<String> mockSchema;
  PubsubLiteSinkWriter<String> writer;

  @Before
  public void setUp() {
    writer = new PubsubLiteSinkWriter<>(mockPublisher, mockSchema);
  }

  @Test
  public void testFlush() throws Exception {
    writer.flush(false);
    verify(mockPublisher).flush();
  }

  @Test
  public void testPublish() throws Exception {
    long timestamp = 10000;
    PubSubMessage message =
        PubSubMessage.newBuilder().setData(ByteString.copyFromUtf8("data")).build();
    when(mockSchema.serialize("message", Instant.ofEpochMilli(timestamp))).thenReturn(message);
    writer.write(
        "message",
        new Context() {
          @Override
          public long currentWatermark() {
            return System.currentTimeMillis();
          }

          @Override
          public Long timestamp() {
            return timestamp;
          }
        });
    verify(mockPublisher).publish(message);
  }
}

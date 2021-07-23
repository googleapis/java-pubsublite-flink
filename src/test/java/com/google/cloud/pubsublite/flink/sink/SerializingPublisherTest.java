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

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.Tuple;
import com.google.cloud.pubsublite.Message;
import com.google.cloud.pubsublite.flink.PubsubLiteSerializationSchema;
import com.google.protobuf.ByteString;
import java.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SerializingPublisherTest {
  @Mock AtLeastOncePublisher<Message> mockPublisher;
  @Mock PubsubLiteSerializationSchema<String> mockSchema;
  SerializingPublisher<String> publisher;

  @Before
  public void setUp() {
    publisher = new SerializingPublisher<>(mockPublisher, mockSchema);
  }

  @Test
  public void testClose() throws Exception {
    publisher.close();
    verify(mockPublisher).close();
  }

  @Test
  public void testCheckpoint() throws Exception {
    publisher.checkpoint();
    verify(mockPublisher).checkpoint();
  }

  @Test
  public void testPublish() throws Exception {
    Instant timestamp = Instant.ofEpochMilli(1000);
    Message message = Message.builder().setData(ByteString.copyFromUtf8("data")).build();
    when(mockSchema.serialize("message", timestamp)).thenReturn(message);
    publisher.publish(Tuple.of("message", timestamp));
    verify(mockPublisher).publish(message);
  }
}

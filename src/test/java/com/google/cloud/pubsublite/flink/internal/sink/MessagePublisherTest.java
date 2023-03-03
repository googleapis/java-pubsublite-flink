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

import static com.google.cloud.pubsublite.internal.testing.UnitTestExamples.exampleOffset;
import static com.google.cloud.pubsublite.internal.testing.UnitTestExamples.examplePartition;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFutures;
import com.google.api.core.SettableApiFuture;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode.Code;
import com.google.cloud.pubsublite.MessageMetadata;
import com.google.cloud.pubsublite.internal.CheckedApiException;
import com.google.cloud.pubsublite.internal.Publisher;
import com.google.cloud.pubsublite.internal.testing.FakeApiService;
import com.google.cloud.pubsublite.proto.PubSubMessage;
import com.google.protobuf.ByteString;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MessagePublisherTest {
  abstract static class FakePublisher extends FakeApiService
      implements Publisher<MessageMetadata> {}

  @Spy FakePublisher fakeInnerPublisher;
  MessagePublisher messagePublisher;

  @Before
  public void setUp() {
    fakeInnerPublisher.startAsync();
    messagePublisher = new MessagePublisher(fakeInnerPublisher, 1);
  }

  @Test
  public void testPublish() throws Exception {
    PubSubMessage message1 = PubSubMessage.newBuilder().build();
    when(fakeInnerPublisher.publish(message1))
        .thenReturn(
            ApiFutures.immediateFuture(MessageMetadata.of(examplePartition(), exampleOffset())));

    messagePublisher.publish(message1);

    messagePublisher.flush();

    verify(fakeInnerPublisher).publish(message1);
  }

  @Test
  public void testSinglePublishFailure() throws Exception {
    PubSubMessage message1 = PubSubMessage.newBuilder().build();
    when(fakeInnerPublisher.publish(message1))
        .thenReturn(
            ApiFutures.immediateFailedFuture(new CheckedApiException(Code.INTERNAL).underlying));
    messagePublisher.publish(message1);
    verify(fakeInnerPublisher).publish(message1);

    assertThrows(ApiException.class, () -> messagePublisher.flush());
  }

  @Test
  public void testCheckpointWithOutstandingPublish() throws Exception {
    PubSubMessage message1 = PubSubMessage.newBuilder().build();
    SettableApiFuture<MessageMetadata> future = SettableApiFuture.create();
    when(fakeInnerPublisher.publish(message1)).thenReturn(future);
    messagePublisher.publish(message1);
    verify(fakeInnerPublisher).publish(message1);

    Future<?> checkpointFuture =
        Executors.newSingleThreadExecutor()
            .submit(
                () -> {
                  try {
                    messagePublisher.flush();
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                });
    // Sleep for a short time so that the checkpoint could complete if it wasn't properly waiting.
    Thread.sleep(50);
    assertThat(checkpointFuture.isDone()).isFalse();
    future.set(MessageMetadata.of(examplePartition(), exampleOffset()));
    checkpointFuture.get();
  }

  @Test
  public void testPublishesOfMaximumSizeSerialized() throws Exception {
    PubSubMessage message1 =
        PubSubMessage.newBuilder().setData(ByteString.copyFromUtf8("one")).build();
    PubSubMessage message2 =
        PubSubMessage.newBuilder().setData(ByteString.copyFromUtf8("two")).build();
    SettableApiFuture<MessageMetadata> firstPublish = SettableApiFuture.create();
    when(fakeInnerPublisher.publish(message1)).thenReturn(firstPublish);
    when(fakeInnerPublisher.publish(message2))
        .thenReturn(
            ApiFutures.immediateFuture(MessageMetadata.of(examplePartition(), exampleOffset())));
    messagePublisher.publish(message1);
    verify(fakeInnerPublisher).publish(message1);

    Future<?> secondPublish =
        Executors.newSingleThreadExecutor()
            .submit(
                () -> {
                  try {
                    messagePublisher.publish(message2);
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                });
    // Sleep for a short time so that the second publish could complete if it wasn't waiting.
    Thread.sleep(50);
    assertThat(secondPublish.isDone()).isFalse();
    firstPublish.set(MessageMetadata.of(examplePartition(), exampleOffset()));
    secondPublish.get();
  }
}

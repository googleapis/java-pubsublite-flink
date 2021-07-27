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

import static com.google.cloud.pubsublite.internal.testing.UnitTestExamples.exampleOffset;
import static com.google.cloud.pubsublite.internal.testing.UnitTestExamples.examplePartition;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFutures;
import com.google.api.core.SettableApiFuture;
import com.google.api.gax.rpc.StatusCode.Code;
import com.google.cloud.pubsublite.Message;
import com.google.cloud.pubsublite.MessageMetadata;
import com.google.cloud.pubsublite.internal.CheckedApiException;
import com.google.cloud.pubsublite.internal.Publisher;
import com.google.cloud.pubsublite.internal.testing.FakeApiService;
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
    messagePublisher = new MessagePublisher(fakeInnerPublisher);
  }

  @Test
  public void testPublish() throws Exception {
    Message message1 = Message.builder().build();
    when(fakeInnerPublisher.publish(message1))
        .thenReturn(
            ApiFutures.immediateFuture(MessageMetadata.of(examplePartition(), exampleOffset())));

    messagePublisher.publish(message1);

    messagePublisher.waitUntilNoOutstandingPublishes();

    verify(fakeInnerPublisher).publish(message1);
  }

  @Test
  public void testSinglePublishFailure() throws Exception {
    Message message1 = Message.builder().build();
    when(fakeInnerPublisher.publish(message1))
        .thenReturn(
            ApiFutures.immediateFailedFuture(new CheckedApiException(Code.INTERNAL).underlying));
    messagePublisher.publish(message1);
    verify(fakeInnerPublisher).publish(message1);

    assertThrows(
        CheckedApiException.class,
        () -> {
          messagePublisher.waitUntilNoOutstandingPublishes();
        });
  }

  @Test
  public void testCheckpointWithOutstandingPublish() throws Exception {
    Message message1 = Message.builder().build();
    SettableApiFuture<MessageMetadata> future = SettableApiFuture.create();
    when(fakeInnerPublisher.publish(message1)).thenReturn(future);
    messagePublisher.publish(message1);
    verify(fakeInnerPublisher).publish(message1);

    Future<?> checkpointFuture =
        Executors.newSingleThreadExecutor()
            .submit(
                () -> {
                  try {
                    messagePublisher.waitUntilNoOutstandingPublishes();
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
}

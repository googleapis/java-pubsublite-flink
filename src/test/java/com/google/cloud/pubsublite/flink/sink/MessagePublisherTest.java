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

  @Spy FakePublisher publisher;
  MessagePublisher messagePublisher;

  @Before
  public void setUp() {
    messagePublisher = new MessagePublisher(publisher);
  }

  @Test
  public void testPublish() throws Exception {
    Message message1 = Message.builder().build();
    when(publisher.publish(message1))
        .thenReturn(ApiFutures.immediateFuture(MessageMetadata.of(examplePartition(), exampleOffset())));

    messagePublisher.publish(message1);

    messagePublisher.waitUntilNoOutstandingPublishes();

    verify(publisher).publish(message1);
  }

  @Test
  public void testSinglePublishFailure() throws Exception {
    Message message1 = Message.builder().build();
    when(publisher.publish(message1))
        .thenReturn(
            ApiFutures.immediateFailedFuture(new CheckedApiException(Code.INTERNAL).underlying));
    messagePublisher.publish(message1);
    verify(publisher).publish(message1);

    assertThrows(CheckedApiException.class, () -> { messagePublisher.waitUntilNoOutstandingPublishes();});
  }

  @Test
  public void testPublisherFailure() throws Exception {
    Message message1 = Message.builder().build();
    publisher.fail(new RuntimeException("failure"));

    assertThrows(CheckedApiException.class, () -> messagePublisher.publish(message1));
  }

  @Test
  public void testCheckpointWithOutstandingPublish() throws Exception {
    Message message1 = Message.builder().build();
    SettableApiFuture<MessageMetadata> future = SettableApiFuture.create();
    when(publisher.publish(message1))
        .thenReturn(future);
    messagePublisher.publish(message1);
    verify(publisher).publish(message1);

    Future<?> checkpointFuture = Executors.newSingleThreadExecutor().submit(() -> {
      try {
        messagePublisher.waitUntilNoOutstandingPublishes();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    Thread.sleep(50);
    assertThat(checkpointFuture.isDone()).isFalse();
    future.set(MessageMetadata.of(examplePartition(), exampleOffset()));
    checkpointFuture.get();
  }




}
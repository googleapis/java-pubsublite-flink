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

import static com.google.cloud.pubsublite.internal.testing.UnitTestExamples.example;
import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiService.State;
import com.google.cloud.pubsublite.MessageMetadata;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.flink.PubsubLiteSerializationSchema;
import com.google.cloud.pubsublite.flink.PubsubLiteSinkSettings;
import com.google.cloud.pubsublite.internal.Publisher;
import com.google.cloud.pubsublite.internal.testing.FakeApiService;
import com.google.protobuf.ByteString;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PublisherCacheTest {
  private static final PubsubLiteSinkSettings<?> SETTINGS =
      PubsubLiteSinkSettings.builder(PubsubLiteSerializationSchema.dataOnly((byte[] x) -> x))
          .setTopicPath(example(TopicPath.class))
          .build();

  abstract static class FakePublisher extends FakeApiService
      implements Publisher<MessageMetadata> {}

  @Mock PublisherCache.PublisherFactory mockFactory;
  PublisherCache cache;

  @Before
  public void setUp() {
    cache = new PublisherCache(mockFactory);
  }

  @Test
  public void testPublisherStarted() {
    FakePublisher pub = spy(FakePublisher.class);
    when(mockFactory.New(SETTINGS)).thenReturn(pub);
    assertThat(cache.get(SETTINGS)).isEqualTo(pub);
    assertThat(pub.state()).isEqualTo(State.RUNNING);
  }

  @Test
  public void testPublisherCached() {
    FakePublisher pub = spy(FakePublisher.class);
    when(mockFactory.New(SETTINGS)).thenReturn(pub);
    assertThat(cache.get(SETTINGS)).isEqualTo(pub);
    PubsubLiteSinkSettings<?> sameTopicSettings =
        PubsubLiteSinkSettings.builder(
                PubsubLiteSerializationSchema.dataOnly(ByteString::toByteArray))
            .setTopicPath(example(TopicPath.class))
            .build();
    assertThat(cache.get(sameTopicSettings)).isEqualTo(pub);
    verify(mockFactory, times(1)).New(any());
  }

  @Test
  public void testFailedPublisherEvicted() throws InterruptedException {
    FakePublisher pub1 = spy(FakePublisher.class);
    FakePublisher pub2 = spy(FakePublisher.class);
    when(mockFactory.New(SETTINGS)).thenReturn(pub1).thenReturn(pub2);
    assertThat(cache.get(SETTINGS)).isEqualTo(pub1);
    pub1.fail(new RuntimeException("failure"));
    while (cache.get(SETTINGS).equals(pub1)) {
      Thread.sleep(100);
    }
    assertThat(cache.get(SETTINGS)).isEqualTo(pub2);
  }

  @Test
  public void testClose() {
    FakePublisher pub1 = spy(FakePublisher.class);
    when(mockFactory.New(SETTINGS)).thenReturn(pub1);
    assertThat(cache.get(SETTINGS)).isEqualTo(pub1);
    cache.close();
    verify(pub1).stopAsync();
  }

  @Test
  public void testSet() {
    FakePublisher pub = spy(FakePublisher.class);
    cache.set(example(TopicPath.class), pub);
    assertThat(cache.get(SETTINGS)).isEqualTo(pub);
  }
}

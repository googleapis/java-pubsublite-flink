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
package com.google.cloud.pubsublite.flink.internal.reader;

import static com.google.cloud.pubsublite.internal.testing.UnitTestExamples.exampleOffset;
import static com.google.cloud.pubsublite.internal.testing.UnitTestExamples.examplePartition;
import static com.google.cloud.pubsublite.internal.testing.UnitTestExamples.exampleSubscriptionPath;
import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.when;

import com.google.cloud.pubsublite.flink.internal.reader.PartitionFinishedCondition.Result;
import com.google.cloud.pubsublite.flink.internal.split.SubscriptionPartitionSplit;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MaybeCompleteSubscriberFactoryTest {
  @Mock PartitionFinishedCondition.Factory mockConditionFactory;
  @Mock PartitionFinishedCondition mockCondition;
  @Mock CompletablePullSubscriber.Factory mockSubscriberFactory;
  @Mock CompletablePullSubscriber mockSubscriber;

  MaybeCompleteSubscriberFactory subscriberFactory;

  @Before
  public void setUp() {
    subscriberFactory =
        new MaybeCompleteSubscriberFactory(mockSubscriberFactory, mockConditionFactory);
  }

  @Test
  public void subscriberAlreadyFinished() throws Exception {
    SubscriptionPartitionSplit split =
        SubscriptionPartitionSplit.create(
            exampleSubscriptionPath(), examplePartition(), exampleOffset());
    when(mockConditionFactory.New(split.subscriptionPath(), split.partition()))
        .thenReturn(mockCondition);
    when(mockCondition.partitionFinished(split.start())).thenReturn(Result.FINISH_AFTER);
    assertThat(subscriberFactory.New(split).isFinished()).isTrue();
  }

  @Test
  public void subscriberNotFinished() throws Exception {
    SubscriptionPartitionSplit split =
        SubscriptionPartitionSplit.create(
            exampleSubscriptionPath(), examplePartition(), exampleOffset());
    when(mockConditionFactory.New(split.subscriptionPath(), split.partition()))
        .thenReturn(mockCondition);
    when(mockCondition.partitionFinished(split.start())).thenReturn(Result.CONTINUE);
    when(mockSubscriberFactory.New(split)).thenReturn(mockSubscriber);
    when(mockSubscriber.isFinished()).thenReturn(false);
    assertThat(subscriberFactory.New(split).isFinished()).isFalse();
  }
}

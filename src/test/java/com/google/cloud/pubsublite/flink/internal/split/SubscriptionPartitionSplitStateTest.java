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
package com.google.cloud.pubsublite.flink.internal.split;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.internal.testing.UnitTestExamples;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SubscriptionPartitionSplitStateTest {

  @Test
  public void testSplitState() {
    SubscriptionPartitionSplit split =
        SubscriptionPartitionSplit.create(
            UnitTestExamples.exampleSubscriptionPath(),
            UnitTestExamples.examplePartition(),
            Offset.of(0));
    SubscriptionPartitionSplitState state = new SubscriptionPartitionSplitState(split);
    state.setCurrent(Offset.of(10));
    SubscriptionPartitionSplit newSplit = state.toSplit();

    assertThat(newSplit.partition()).isEqualTo(split.partition());
    assertThat(newSplit.subscriptionPath()).isEqualTo(split.subscriptionPath());
    assertThat(newSplit.start()).isEqualTo(Offset.of(10));
  }
}

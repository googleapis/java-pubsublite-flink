/*
 * Copyright 2020 Google LLC
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
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.flink.proto.SubscriptionPartitionSplitProto;
import com.google.cloud.pubsublite.internal.testing.UnitTestExamples;
import org.junit.Test;

public class SubscriptionPartitionSplitTest {

  @Test
  public void testToProtoRoundTrip() {
    SubscriptionPartitionSplit split =
        SubscriptionPartitionSplit.create(
            UnitTestExamples.exampleSubscriptionPath(),
            UnitTestExamples.examplePartition(),
            UnitTestExamples.exampleOffset());
    assertThat(SubscriptionPartitionSplit.fromProto(split.toProto())).isEqualTo(split);
  }

  @Test
  public void testFromProtoRoundTrip() {
    SubscriptionPartitionSplitProto proto =
        SubscriptionPartitionSplitProto.newBuilder()
            .setPartition(UnitTestExamples.examplePartition().value())
            .setSubscription(UnitTestExamples.exampleSubscriptionPath().toString())
            .setStart(
                SubscriptionPartitionSplitProto.Cursor.newBuilder()
                    .setOffset(UnitTestExamples.exampleOffset().value()))
            .build();

    assertThat(SubscriptionPartitionSplit.fromProto(proto).toProto()).isEqualTo(proto);
  }

  @Test
  public void testSplitId() {
    String subscription = "projects/5/locations/us-central1-b/subscriptions/foo";
    SubscriptionPartitionSplit split =
        SubscriptionPartitionSplit.create(
            SubscriptionPath.parse(subscription), Partition.of(1), Offset.of(2));
    assertThat(split.splitId()).isEqualTo(String.format("1-2-%s", subscription));
  }
}

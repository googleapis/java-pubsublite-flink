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

package com.google.cloud.pubsublite.flink.split;

import com.google.auto.value.AutoValue;
import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.flink.proto.SubscriptionPartitionSplitProto;
import org.apache.flink.api.connector.source.SourceSplit;

/** A split representing an offset within a subscription partition. */
@AutoValue
public abstract class SubscriptionPartitionSplit implements SourceSplit {

  public abstract SubscriptionPath subscriptionPath();

  public abstract Partition partition();

  public abstract Offset start();

  public static SubscriptionPartitionSplit create(
      SubscriptionPath subscriptionPath, Partition partition, Offset start) {
    return new AutoValue_SubscriptionPartitionSplit(subscriptionPath, partition, start);
  }

  public static SubscriptionPartitionSplit fromProto(SubscriptionPartitionSplitProto proto) {
    return SubscriptionPartitionSplit.create(
        SubscriptionPath.parse(proto.getSubscription()),
        Partition.of(proto.getPartition()),
        Offset.of(proto.getStart().getOffset()));
  }

  public SubscriptionPartitionSplitProto toProto() {
    return SubscriptionPartitionSplitProto.newBuilder()
        .setSubscription(subscriptionPath().toString())
        .setPartition(partition().value())
        .setStart(SubscriptionPartitionSplitProto.Cursor.newBuilder().setOffset(start().value()))
        .build();
  }

  @Override
  public String splitId() {
    return String.format(
        "%s-%s-%s", partition().value(), start().value(), subscriptionPath().toString());
  }
}

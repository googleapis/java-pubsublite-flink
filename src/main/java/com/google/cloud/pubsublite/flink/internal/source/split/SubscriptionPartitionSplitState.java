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
package com.google.cloud.pubsublite.flink.internal.source.split;

import com.google.cloud.pubsublite.Offset;

public final class SubscriptionPartitionSplitState {
  final SubscriptionPartitionSplit split;
  Offset current;

  public SubscriptionPartitionSplitState(SubscriptionPartitionSplit split) {
    this.split = split;
    this.current = split.start();
  }

  public void setCurrent(Offset offset) {
    current = offset;
  }

  public SubscriptionPartitionSplit toSplit() {
    return SubscriptionPartitionSplit.create(split.subscriptionPath(), split.partition(), current);
  }
}

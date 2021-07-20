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
package com.google.cloud.pubsublite.flink.reader;

import com.google.cloud.pubsublite.flink.split.SubscriptionPartitionSplitState;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

/**
 * The pubsub lite record emitter emits records to the source output and also tracks the position of
 * the source reader within the split by updating the current offset on the split state.
 */
public class PubsubLiteRecordEmitter<T>
    implements RecordEmitter<Record<T>, T, SubscriptionPartitionSplitState> {
  @Override
  public void emitRecord(
      Record<T> record,
      SourceOutput<T> sourceOutput,
      SubscriptionPartitionSplitState subscriptionPartitionSplitState) {
    if (record.value().isPresent()) {
      sourceOutput.collect(record.value().get(), record.timestamp().toEpochMilli());
    }
    // Update the position of the source reader within the split.
    subscriptionPartitionSplitState.setCurrent(record.offset());
  }
}

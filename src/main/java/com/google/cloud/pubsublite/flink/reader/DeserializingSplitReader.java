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

import com.google.cloud.pubsublite.SequencedMessage;
import com.google.cloud.pubsublite.flink.MessageTimestampExtractor;
import com.google.cloud.pubsublite.flink.PubsubLiteDeserializationSchema;
import com.google.cloud.pubsublite.flink.split.SubscriptionPartitionSplit;
import java.io.IOException;
import java.util.Optional;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;

public class DeserializingSplitReader<T>
    implements SplitReader<Record<T>, SubscriptionPartitionSplit> {
  final SplitReader<SequencedMessage, SubscriptionPartitionSplit> reader;
  final PubsubLiteDeserializationSchema<T> schema;
  final MessageTimestampExtractor selector;

  public DeserializingSplitReader(
      SplitReader<SequencedMessage, SubscriptionPartitionSplit> reader,
      PubsubLiteDeserializationSchema<T> schema,
      MessageTimestampExtractor selector) {
    this.reader = reader;
    this.schema = schema;
    this.selector = selector;
  }

  @Override
  public RecordsBySplits<Record<T>> fetch() throws IOException {
    RecordsBySplits.Builder<Record<T>> builder = new RecordsBySplits.Builder<>();
    RecordsWithSplitIds<SequencedMessage> fetch = reader.fetch();
    for (String split = fetch.nextSplit(); split != null; split = fetch.nextSplit()) {
      for (SequencedMessage m = fetch.nextRecordFromSplit();
          m != null;
          m = fetch.nextRecordFromSplit()) {
        try {
          T value = schema.deserialize(m);
          long timestamp = selector.timestamp(m);
          builder.add(split, Record.create(Optional.ofNullable(value), m.offset(), timestamp));
        } catch (Exception e) {
          throw new IOException(e);
        }
      }
    }
    builder.addFinishedSplits(fetch.finishedSplits());
    return builder.build();
  }

  @Override
  public void handleSplitsChanges(SplitsChange<SubscriptionPartitionSplit> splitsChange) {
    reader.handleSplitsChanges(splitsChange);
  }

  @Override
  public void wakeUp() {
    reader.wakeUp();
  }

  @Override
  public void close() throws Exception {
    reader.close();
  }
}

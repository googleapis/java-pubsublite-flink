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
package com.google.cloud.pubsublite.flink.internal.source.reader;

import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.flink.MessageTimestampExtractor;
import com.google.cloud.pubsublite.flink.PubsubLiteDeserializationSchema;
import com.google.cloud.pubsublite.flink.internal.source.split.SubscriptionPartitionSplit;
import com.google.cloud.pubsublite.proto.SequencedMessage;
import com.google.common.collect.Multimap;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;

public class DeserializingSplitReader<T>
    implements SplitReader<Record<T>, SubscriptionPartitionSplit> {
  final SplitReader<SequencedMessage, SubscriptionPartitionSplit> underlying;
  final PubsubLiteDeserializationSchema<T> schema;
  final MessageTimestampExtractor extractor;

  public DeserializingSplitReader(
      SplitReader<SequencedMessage, SubscriptionPartitionSplit> underlying,
      PubsubLiteDeserializationSchema<T> schema,
      MessageTimestampExtractor extractor) {
    this.underlying = underlying;
    this.schema = schema;
    this.extractor = extractor;
  }

  @Override
  public RecordsBySplits<Record<T>> fetch() throws IOException {
    RecordsBySplits.Builder<Record<T>> builder = new RecordsBySplits.Builder<>();
    RecordsWithSplitIds<SequencedMessage> fetch = underlying.fetch();
    Multimap<String, SequencedMessage> messageMap = ReaderUtils.recordWithSplitsToMap(fetch);
    for (Map.Entry<String, SequencedMessage> entry : messageMap.entries()) {
      String split = entry.getKey();
      SequencedMessage message = entry.getValue();
      try {
        @Nullable T value = schema.deserialize(message);
        Instant timestamp = extractor.timestamp(message);
        builder.add(
            split,
            Record.create(
                Optional.ofNullable(value), Offset.of(message.getCursor().getOffset()), timestamp));
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
    builder.addFinishedSplits(fetch.finishedSplits());
    return builder.build();
  }

  @Override
  public void handleSplitsChanges(SplitsChange<SubscriptionPartitionSplit> splitsChange) {
    underlying.handleSplitsChanges(splitsChange);
  }

  @Override
  public void wakeUp() {
    underlying.wakeUp();
  }

  @Override
  public void close() throws Exception {
    underlying.close();
  }
}

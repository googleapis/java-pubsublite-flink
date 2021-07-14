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
package com.google.cloud.pubsublite.flink;

import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.SequencedMessage;
import com.google.cloud.pubsublite.proto.Cursor;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;

public class TestUtilities {
  public static SequencedMessage messageFromOffset(Offset offset) {
    return SequencedMessage.fromProto(
        com.google.cloud.pubsublite.proto.SequencedMessage.newBuilder()
            .setCursor(Cursor.newBuilder().setOffset(offset.value()))
            .build());
  }

  public static <T> Multimap<String, T> recordWithSplitsToMap(RecordsWithSplitIds<T> records) {
    Multimap<String, T> map = HashMultimap.create();
    for (String split = records.nextSplit(); split != null; split = records.nextSplit()) {
      for (T m = records.nextRecordFromSplit(); m != null; m = records.nextRecordFromSplit()) {
        map.put(split, m);
      }
    }
    return map;
  }
}
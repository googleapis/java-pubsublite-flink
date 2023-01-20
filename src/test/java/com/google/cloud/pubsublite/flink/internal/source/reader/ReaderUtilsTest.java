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

import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ReaderUtilsTest {

  @Test
  public void testConversion() {
    ImmutableListMultimap<String, Integer> map =
        ImmutableListMultimap.<String, Integer>builder()
            .putAll("evens", 2, 4, 6)
            .putAll("odds", 1, 3, 5)
            .build();
    RecordsBySplits<Integer> recordsWithSplitIds =
        new RecordsBySplits<>(map.asMap(), ImmutableSet.of("foo"));
    assertThat(ReaderUtils.recordWithSplitsToMap(recordsWithSplitIds))
        .containsExactlyEntriesIn(map);
  }
}

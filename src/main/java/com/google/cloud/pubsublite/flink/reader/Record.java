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

import com.google.auto.value.AutoValue;
import com.google.cloud.pubsublite.Offset;
import java.time.Instant;
import java.util.Optional;

@AutoValue
public abstract class Record<T> {
  public abstract Optional<T> value();

  public abstract Offset offset();

  public abstract Instant timestamp();

  public static <T> Record<T> create(Optional<T> value, Offset offset, Instant timestamp) {
    return new AutoValue_Record<>(value, offset, timestamp);
  }
}

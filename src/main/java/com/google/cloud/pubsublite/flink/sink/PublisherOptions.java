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
package com.google.cloud.pubsublite.flink.sink;

import com.google.auto.value.AutoValue;
import com.google.cloud.pubsublite.MessageMetadata;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.internal.Publisher;
import javax.annotation.Nullable;
import org.apache.flink.util.function.SerializableSupplier;

@AutoValue
public abstract class PublisherOptions {

  public abstract TopicPath topicPath();

  public abstract @Nullable SerializableSupplier<Publisher<MessageMetadata>> publisherSupplier();

  public static PublisherOptions create(
      TopicPath path, @Nullable SerializableSupplier<Publisher<MessageMetadata>> supplier) {
    return new AutoValue_PublisherOptions(path, supplier);
  }
}

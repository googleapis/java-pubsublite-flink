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
package com.google.cloud.pubsublite.flink.internal.sink;

import com.google.cloud.pubsublite.MessageMetadata;
import com.google.cloud.pubsublite.flink.PubsubLiteSinkSettings;
import com.google.cloud.pubsublite.internal.Publisher;
import com.google.common.annotations.VisibleForTesting;

public class PerServerPublisherCache {
  private static final PublisherCache cache =
      new PublisherCache(PerServerPublisherCache::newPublisher);

  static {
    Runtime.getRuntime().addShutdownHook(new Thread(cache::close));
  }

  private static Publisher<MessageMetadata> newPublisher(PubsubLiteSinkSettings<?> options) {
    SinkAssembler<?> assembler = new SinkAssembler<>(options);
    return assembler.newPublisher();
  }

  public static Publisher<MessageMetadata> getOrCreate(PubsubLiteSinkSettings<?> options) {
    return cache.get(options);
  }

  @VisibleForTesting
  public static PublisherCache getCache() {
    return cache;
  }
}

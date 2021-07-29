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

import static com.google.cloud.pubsublite.internal.ExtractStatus.toCanonical;
import static com.google.cloud.pubsublite.internal.wire.ServiceClients.addDefaultMetadata;
import static com.google.cloud.pubsublite.internal.wire.ServiceClients.addDefaultSettings;

import com.google.cloud.pubsublite.AdminClient;
import com.google.cloud.pubsublite.AdminClientSettings;
import com.google.cloud.pubsublite.CloudRegion;
import com.google.cloud.pubsublite.MessageMetadata;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.cloudpubsub.PublisherSettings;
import com.google.cloud.pubsublite.internal.Publisher;
import com.google.cloud.pubsublite.internal.wire.PartitionCountWatchingPublisherSettings;
import com.google.cloud.pubsublite.internal.wire.PubsubContext;
import com.google.cloud.pubsublite.internal.wire.RoutingMetadata;
import com.google.cloud.pubsublite.internal.wire.SinglePartitionPublisherBuilder;
import com.google.cloud.pubsublite.v1.PublisherServiceClient;
import com.google.cloud.pubsublite.v1.PublisherServiceSettings;
import com.google.common.annotations.VisibleForTesting;

public class PerServerPublisherCache {
  private static final PublisherCache<PublisherOptions> cache =
      new PublisherCache<>(PerServerPublisherCache::newPublisher);

  private static PublisherServiceClient newServiceClient(TopicPath topic, Partition partition) {
    PublisherServiceSettings.Builder settingsBuilder = PublisherServiceSettings.newBuilder();
    settingsBuilder =
        addDefaultMetadata(
            PubsubContext.of(PubsubContext.Framework.of("FLINK")),
            RoutingMetadata.of(topic, partition),
            settingsBuilder);
    try {
      return PublisherServiceClient.create(
          addDefaultSettings(topic.location().region(), settingsBuilder));
    } catch (Throwable t) {
      throw toCanonical(t).underlying;
    }
  }

  private static AdminClient getAdminClient(CloudRegion region) {
    return AdminClient.create(AdminClientSettings.newBuilder().setRegion(region).build());
  }

  private static Publisher<MessageMetadata> newPublisher(PublisherOptions options) {
    return PartitionCountWatchingPublisherSettings.newBuilder()
        .setTopic(options.topicPath())
        .setPublisherFactory(
            partition ->
                SinglePartitionPublisherBuilder.newBuilder()
                    .setTopic(options.topicPath())
                    .setPartition(partition)
                    .setServiceClient(newServiceClient(options.topicPath(), partition))
                    .setBatchingSettings(PublisherSettings.DEFAULT_BATCHING_SETTINGS)
                    .build())
        .setAdminClient(getAdminClient(options.topicPath().location().region()))
        .build()
        .instantiate();
  }

  public static Publisher<MessageMetadata> getOrCreate(PublisherOptions options) {
    return cache.get(options);
  }

  @VisibleForTesting
  public static PublisherCache<PublisherOptions> getCache() {
    return cache;
  }
}

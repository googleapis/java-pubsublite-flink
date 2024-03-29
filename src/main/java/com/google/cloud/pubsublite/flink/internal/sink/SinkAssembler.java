/*
 * Copyright 2022 Google LLC
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

import static com.google.cloud.pubsublite.internal.ExtractStatus.toCanonical;
import static com.google.cloud.pubsublite.internal.wire.ServiceClients.addDefaultSettings;
import static com.google.cloud.pubsublite.internal.wire.ServiceClients.getCallContext;

import com.google.api.gax.rpc.ApiCallContext;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsublite.AdminClient;
import com.google.cloud.pubsublite.AdminClientSettings;
import com.google.cloud.pubsublite.CloudRegion;
import com.google.cloud.pubsublite.MessageMetadata;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.cloudpubsub.PublisherSettings;
import com.google.cloud.pubsublite.flink.PubsubLiteSinkSettings;
import com.google.cloud.pubsublite.internal.Publisher;
import com.google.cloud.pubsublite.internal.wire.PartitionCountWatchingPublisherSettings;
import com.google.cloud.pubsublite.internal.wire.PartitionPublisherFactory;
import com.google.cloud.pubsublite.internal.wire.PubsubContext;
import com.google.cloud.pubsublite.internal.wire.PubsubContext.Framework;
import com.google.cloud.pubsublite.internal.wire.RoutingMetadata;
import com.google.cloud.pubsublite.internal.wire.SinglePartitionPublisherBuilder;
import com.google.cloud.pubsublite.v1.AdminServiceClient;
import com.google.cloud.pubsublite.v1.AdminServiceSettings;
import com.google.cloud.pubsublite.v1.PublisherServiceClient;
import com.google.cloud.pubsublite.v1.PublisherServiceSettings;

public class SinkAssembler<InputT> {
  private static final Framework FRAMEWORK = Framework.of("FLINK");

  private PublisherServiceClient newPublisherServiceClient() throws ApiException {
    try {
      PublisherServiceSettings.Builder settingsBuilder = PublisherServiceSettings.newBuilder();
      return PublisherServiceClient.create(addDefaultSettings(extractRegion(), settingsBuilder));
    } catch (Throwable t) {
      throw toCanonical(t).underlying;
    }
  }

  public CloudRegion extractRegion() {
    return settings.topicPath().location().extractRegion();
  }

  public Publisher<MessageMetadata> newPublisher() {
    PublisherServiceClient client = newPublisherServiceClient();
    return PartitionCountWatchingPublisherSettings.newBuilder()
        .setTopic(settings.topicPath())
        .setPublisherFactory(
            new PartitionPublisherFactory() {
              @Override
              public Publisher<MessageMetadata> newPublisher(Partition partition)
                  throws ApiException {
                return SinglePartitionPublisherBuilder.newBuilder()
                    .setTopic(settings.topicPath())
                    .setPartition(partition)
                    .setBatchingSettings(PublisherSettings.DEFAULT_BATCHING_SETTINGS)
                    .setStreamFactory(
                        responseObserver -> {
                          ApiCallContext context =
                              getCallContext(
                                  PubsubContext.of(FRAMEWORK),
                                  RoutingMetadata.of(settings.topicPath(), partition));
                          return client.publishCallable().splitCall(responseObserver, context);
                        })
                    .build();
              }

              @Override
              public void close() {
                client.close();
              }
            })
        .setAdminClient(newAdminClient(extractRegion()))
        .build()
        .instantiate();
  }

  private static AdminClient newAdminClient(CloudRegion region) throws ApiException {
    try {
      AdminServiceSettings.Builder settingsBuilder = AdminServiceSettings.newBuilder();
      return AdminClient.create(
          AdminClientSettings.newBuilder()
              .setServiceClient(
                  AdminServiceClient.create(addDefaultSettings(region, settingsBuilder)))
              .setRegion(region)
              .build());
    } catch (Throwable t) {
      throw toCanonical(t).underlying;
    }
  }

  private final PubsubLiteSinkSettings<InputT> settings;

  public SinkAssembler(PubsubLiteSinkSettings<InputT> settings) {
    this.settings = settings;
  }
}

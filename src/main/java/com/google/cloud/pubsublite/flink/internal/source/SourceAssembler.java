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
package com.google.cloud.pubsublite.flink.internal.source;

import static com.google.cloud.pubsublite.internal.ExtractStatus.toCanonical;
import static com.google.cloud.pubsublite.internal.wire.ServiceClients.addDefaultSettings;
import static com.google.cloud.pubsublite.internal.wire.ServiceClients.getCallContext;
import static java.util.concurrent.TimeUnit.MINUTES;

import com.google.api.gax.rpc.ApiCallContext;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsublite.AdminClient;
import com.google.cloud.pubsublite.AdminClientSettings;
import com.google.cloud.pubsublite.CloudRegion;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.flink.PubsubLiteSourceSettings;
import com.google.cloud.pubsublite.flink.internal.source.reader.DeserializingSplitReader;
import com.google.cloud.pubsublite.flink.internal.source.reader.MessageSplitReader;
import com.google.cloud.pubsublite.flink.internal.source.reader.PullSubscriberFactory;
import com.google.cloud.pubsublite.flink.internal.source.reader.Record;
import com.google.cloud.pubsublite.flink.internal.source.split.SubscriptionPartitionSplit;
import com.google.cloud.pubsublite.internal.BlockingPullSubscriberImpl;
import com.google.cloud.pubsublite.internal.CursorClient;
import com.google.cloud.pubsublite.internal.CursorClientSettings;
import com.google.cloud.pubsublite.internal.wire.Committer;
import com.google.cloud.pubsublite.internal.wire.CommitterSettings;
import com.google.cloud.pubsublite.internal.wire.PubsubContext;
import com.google.cloud.pubsublite.internal.wire.PubsubContext.Framework;
import com.google.cloud.pubsublite.internal.wire.RoutingMetadata;
import com.google.cloud.pubsublite.internal.wire.SubscriberBuilder;
import com.google.cloud.pubsublite.internal.wire.SubscriberFactory;
import com.google.cloud.pubsublite.proto.Cursor;
import com.google.cloud.pubsublite.proto.SeekRequest;
import com.google.cloud.pubsublite.v1.AdminServiceClient;
import com.google.cloud.pubsublite.v1.AdminServiceSettings;
import com.google.cloud.pubsublite.v1.CursorServiceClient;
import com.google.cloud.pubsublite.v1.CursorServiceSettings;
import com.google.cloud.pubsublite.v1.SubscriberServiceClient;
import com.google.cloud.pubsublite.v1.SubscriberServiceSettings;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;

public final class SourceAssembler<OutputT> {
  private static final Framework FRAMEWORK = Framework.of("FLINK");
  private static final ConcurrentHashMap<SubscriptionPath, TopicPath> KNOWN_PATHS =
      new ConcurrentHashMap<>();
  private static final ConcurrentHashMap<CloudRegion, SubscriberServiceClient> SUB_CLIENTS =
      new ConcurrentHashMap<>();
  private static final ConcurrentHashMap<CloudRegion, CursorServiceClient> CURSOR_CLIENTS =
      new ConcurrentHashMap<>();
  private static final ConcurrentHashMap<CloudRegion, AdminServiceClient> ADMIN_CLIENTS =
      new ConcurrentHashMap<>();

  private TopicPath lookupTopicPath(SubscriptionPath subscriptionPath) {
    AdminClient adminClient = getUnownedAdminClient();
    try {
      return TopicPath.parse(
          adminClient.getSubscription(subscriptionPath).get(1, MINUTES).getTopic());
    } catch (Throwable t) {
      throw toCanonical(t).underlying;
    }
  }

  public TopicPath getTopicPath() {
    return KNOWN_PATHS.computeIfAbsent(settings.subscriptionPath(), this::lookupTopicPath);
  }

  public CloudRegion extractRegion() {
    return settings.subscriptionPath().location().extractRegion();
  }

  private static SubscriberServiceClient newSubscriberServiceClient(CloudRegion region)
      throws ApiException {
    try {
      SubscriberServiceSettings.Builder settingsBuilder = SubscriberServiceSettings.newBuilder();
      return SubscriberServiceClient.create(addDefaultSettings(region, settingsBuilder));
    } catch (Throwable t) {
      throw toCanonical(t).underlying;
    }
  }

  private SubscriberServiceClient getSubscriberServiceClient() {
    return SUB_CLIENTS.computeIfAbsent(
        extractRegion(), SourceAssembler::newSubscriberServiceClient);
  }

  private static CursorServiceClient newCursorClient(CloudRegion region) throws ApiException {
    try {
      CursorServiceSettings.Builder settingsBuilder = CursorServiceSettings.newBuilder();
      return CursorServiceClient.create(addDefaultSettings(region, settingsBuilder));
    } catch (Throwable t) {
      throw toCanonical(t).underlying;
    }
  }

  private CursorServiceClient getCursorServiceClient() {
    return CURSOR_CLIENTS.computeIfAbsent(extractRegion(), SourceAssembler::newCursorClient);
  }

  private static AdminServiceClient newAdminClient(CloudRegion region) throws ApiException {
    try {
      AdminServiceSettings.Builder settingsBuilder = AdminServiceSettings.newBuilder();
      return AdminServiceClient.create(addDefaultSettings(region, settingsBuilder));
    } catch (Throwable t) {
      throw toCanonical(t).underlying;
    }
  }

  private AdminServiceClient getAdminServiceClient() {
    return ADMIN_CLIENTS.computeIfAbsent(extractRegion(), SourceAssembler::newAdminClient);
  }

  private SubscriberFactory getSubscriberFactory(SubscriptionPartitionSplit split) {
    SubscriberServiceClient client = getSubscriberServiceClient();
    return (consumer) ->
        SubscriberBuilder.newBuilder()
            .setMessageConsumer(consumer)
            .setSubscriptionPath(split.subscriptionPath())
            .setPartition(split.partition())
            .setStreamFactory(
                responseObserver -> {
                  ApiCallContext context =
                      getCallContext(
                          PubsubContext.of(FRAMEWORK),
                          RoutingMetadata.of(split.subscriptionPath(), split.partition()));
                  return client.subscribeCallable().splitCall(responseObserver, context);
                })
            .setInitialLocation(
                SeekRequest.newBuilder()
                    .setCursor(Cursor.newBuilder().setOffset(split.start().value()).build())
                    .build())
            .build();
  }

  PullSubscriberFactory getSplitStateFactory() {
    return (split) ->
        new BlockingPullSubscriberImpl(getSubscriberFactory(split), settings.flowControlSettings());
  }

  public Supplier<SplitReader<Record<OutputT>, SubscriptionPartitionSplit>>
      getSplitReaderSupplier() {
    return () ->
        new DeserializingSplitReader<>(
            new MessageSplitReader(getSplitStateFactory()),
            settings.deserializationSchema(),
            settings.timestampSelector());
  }

  public Committer getCommitter(Partition partition) {
    CursorServiceClient client = getCursorServiceClient();
    return CommitterSettings.newBuilder()
        .setSubscriptionPath(settings.subscriptionPath())
        .setPartition(partition)
        .setStreamFactory(
            responseObserver -> client.streamingCommitCursorCallable().splitCall(responseObserver))
        .build()
        .instantiate();
  }

  public CursorClient getUnownedCursorClient() {
    return CursorClient.create(
        CursorClientSettings.newBuilder()
            .setRegion(extractRegion())
            .setServiceClient(getCursorServiceClient())
            .build());
  }

  public AdminClient getUnownedAdminClient() {
    return AdminClient.create(
        AdminClientSettings.newBuilder()
            .setRegion(extractRegion())
            .setServiceClient(getAdminServiceClient())
            .build());
  }

  private final PubsubLiteSourceSettings<OutputT> settings;

  public SourceAssembler(PubsubLiteSourceSettings<OutputT> settings) {
    this.settings = settings;
  }
}

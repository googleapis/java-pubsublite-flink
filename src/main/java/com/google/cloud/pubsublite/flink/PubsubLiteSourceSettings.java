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

import static com.google.cloud.pubsublite.internal.ExtractStatus.toCanonical;
import static com.google.cloud.pubsublite.internal.wire.ServiceClients.addDefaultMetadata;
import static com.google.cloud.pubsublite.internal.wire.ServiceClients.addDefaultSettings;

import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ApiException;
import com.google.auto.value.AutoValue;
import com.google.cloud.pubsublite.AdminClient;
import com.google.cloud.pubsublite.AdminClientSettings;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.SequencedMessage;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.cloudpubsub.FlowControlSettings;
import com.google.cloud.pubsublite.flink.internal.reader.*;
import com.google.cloud.pubsublite.flink.internal.split.SubscriptionPartitionSplit;
import com.google.cloud.pubsublite.internal.BlockingPullSubscriberImpl;
import com.google.cloud.pubsublite.internal.CursorClient;
import com.google.cloud.pubsublite.internal.CursorClientSettings;
import com.google.cloud.pubsublite.internal.TopicStatsClient;
import com.google.cloud.pubsublite.internal.TopicStatsClientSettings;
import com.google.cloud.pubsublite.internal.wire.PubsubContext;
import com.google.cloud.pubsublite.internal.wire.RoutingMetadata;
import com.google.cloud.pubsublite.internal.wire.SubscriberBuilder;
import com.google.cloud.pubsublite.internal.wire.SubscriberFactory;
import com.google.cloud.pubsublite.proto.Cursor;
import com.google.cloud.pubsublite.proto.SeekRequest;
import com.google.cloud.pubsublite.v1.SubscriberServiceClient;
import com.google.cloud.pubsublite.v1.SubscriberServiceSettings;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.Serializable;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.util.function.SerializableSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
public abstract class PubsubLiteSourceSettings<OutputT> implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(PubsubLiteSourceSettings.class);
  private static final long serialVersionUID = 3206181560865850636L;

  public static <OutputT> Builder<OutputT> builder(
      PubsubLiteDeserializationSchema<OutputT> schema) {
    return new AutoValue_PubsubLiteSourceSettings.Builder<OutputT>()
        .setDeserializationSchema(schema)
        .setBoundedness(Boundedness.CONTINUOUS_UNBOUNDED)
        .setTimestampSelector(MessageTimestampExtractor.publishTimeExtractor())
        .setStopCondition(StopCondition.continueIndefinitely());
  }

  public static Builder<SequencedMessage> messagesBuilder() {
    return builder(PubsubLiteDeserializationSchema.sequencedMessageSchema());
  }

  // Required
  public abstract SubscriptionPath subscriptionPath();

  // Required
  public abstract FlowControlSettings flowControlSettings();

  // Optional
  public abstract Boundedness boundedness();

  // Optional
  public abstract MessageTimestampExtractor timestampSelector();

  // Optional
  public abstract StopCondition stopCondition();

  // Internal
  abstract PubsubLiteDeserializationSchema<OutputT> deserializationSchema();

  abstract @Nullable SerializableSupplier<AdminClient> adminClientSupplier();

  abstract @Nullable SerializableSupplier<CursorClient> cursorClientSupplier();

  abstract @Nullable SerializableSupplier<TopicStatsClient> topicStatsClientSupplier();

  AdminClient getAdminClient() {
    if (adminClientSupplier() != null) {
      return adminClientSupplier().get();
    }
    return AdminClient.create(
        AdminClientSettings.newBuilder()
            .setRegion(subscriptionPath().location().extractRegion())
            .build());
  }

  TopicStatsClient getTopicStatsClient() {
    if (topicStatsClientSupplier() != null) {
      return topicStatsClientSupplier().get();
    }
    return TopicStatsClient.create(
        TopicStatsClientSettings.newBuilder()
            .setRegion(subscriptionPath().location().extractRegion())
            .build());
  }

  CursorClient getCursorClient() {
    if (cursorClientSupplier() != null) {
      return cursorClientSupplier().get();
    }
    return CursorClient.create(
        CursorClientSettings.newBuilder()
            .setRegion(subscriptionPath().location().extractRegion())
            .build());
  }

  private static SubscriberServiceClient newSubscriberServiceClient(
      SubscriptionPath path, Partition partition) throws ApiException {
    try {
      SubscriberServiceSettings.Builder settingsBuilder = SubscriberServiceSettings.newBuilder();
      settingsBuilder =
          addDefaultMetadata(
              PubsubContext.of(PubsubContext.Framework.of("FLINK")),
              RoutingMetadata.of(path, partition),
              settingsBuilder);
      return SubscriberServiceClient.create(
          addDefaultSettings(path.location().extractRegion(), settingsBuilder));
    } catch (Throwable t) {
      throw toCanonical(t).underlying;
    }
  }

  private static SubscriberFactory getSubscriberFactory(SubscriptionPartitionSplit split) {
    return (consumer) ->
        SubscriberBuilder.newBuilder()
            .setSubscriptionPath(split.subscriptionPath())
            .setPartition(split.partition())
            .setServiceClient(
                newSubscriberServiceClient(split.subscriptionPath(), split.partition()))
            .setMessageConsumer(consumer)
            .setInitialLocation(
                SeekRequest.newBuilder()
                    .setCursor(Cursor.newBuilder().setOffset(split.start().value()).build())
                    .build())
            .build();
  }

  CompletablePullSubscriber.Factory getSplitStateFactory() {
    PartitionFinishedCondition.Factory conditionFactory = stopCondition().toFinishCondition();
    return new MaybeCompleteSubscriberFactory(
        (split) ->
            new CompletablePullSubscriberImpl(
                new BlockingPullSubscriberImpl(getSubscriberFactory(split), flowControlSettings()),
                conditionFactory.New(split.subscriptionPath(), split.partition())),
        conditionFactory);
  }

  Supplier<SplitReader<Record<OutputT>, SubscriptionPartitionSplit>> getSplitReaderSupplier() {
    return () ->
        new DeserializingSplitReader<>(
            new MessageSplitReader(getSplitStateFactory()),
            deserializationSchema(),
            timestampSelector());
  }

  Consumer<SubscriptionPartitionSplit> getCursorCommitter() {
    CursorClient client = getCursorClient();
    return (SubscriptionPartitionSplit split) -> {
      ApiFutures.addCallback(
          client.commitCursor(split.subscriptionPath(), split.partition(), split.start()),
          new ApiFutureCallback<Void>() {
            @Override
            public void onFailure(Throwable throwable) {
              LOG.error("Failed to commit cursor to Pub/Sub Lite ", throwable);
            }

            @Override
            public void onSuccess(Void unused) {}
          },
          MoreExecutors.directExecutor());
    };
  }

  @AutoValue.Builder
  public abstract static class Builder<OutputT> {
    // Required
    public abstract Builder<OutputT> setSubscriptionPath(SubscriptionPath path);

    // Required
    public abstract Builder<OutputT> setFlowControlSettings(FlowControlSettings settings);

    // Optional
    public abstract Builder<OutputT> setBoundedness(Boundedness value);

    // Optional
    public abstract Builder<OutputT> setTimestampSelector(MessageTimestampExtractor value);

    // Optional
    public abstract Builder<OutputT> setStopCondition(StopCondition value);

    abstract Builder<OutputT> setDeserializationSchema(
        PubsubLiteDeserializationSchema<OutputT> schema);

    abstract Builder<OutputT> setAdminClientSupplier(SerializableSupplier<AdminClient> value);

    abstract Builder<OutputT> setCursorClientSupplier(SerializableSupplier<CursorClient> value);

    abstract Builder<OutputT> setTopicStatsClientSupplier(
        SerializableSupplier<TopicStatsClient> value);

    public abstract PubsubLiteSourceSettings<OutputT> autoBuild();

    public PubsubLiteSourceSettings<OutputT> build() {
      PubsubLiteSourceSettings<OutputT> settings = autoBuild();
      setStopCondition(
          settings
              .stopCondition()
              .canonicalize(
                  settings.subscriptionPath(),
                  settings::getAdminClient,
                  settings::getTopicStatsClient));
      return autoBuild();
    }
  }
}

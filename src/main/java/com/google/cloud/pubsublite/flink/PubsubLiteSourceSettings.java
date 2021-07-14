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

import com.google.api.gax.rpc.ApiException;
import com.google.auto.value.AutoValue;
import com.google.cloud.pubsublite.AdminClient;
import com.google.cloud.pubsublite.AdminClientSettings;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.cloudpubsub.FlowControlSettings;
import com.google.cloud.pubsublite.flink.reader.*;
import com.google.cloud.pubsublite.flink.split.SubscriptionPartitionSplit;
import com.google.cloud.pubsublite.internal.BlockingPullSubscriber;
import com.google.cloud.pubsublite.internal.BlockingPullSubscriberImpl;
import com.google.cloud.pubsublite.internal.CursorClient;
import com.google.cloud.pubsublite.internal.CursorClientSettings;
import com.google.cloud.pubsublite.internal.wire.PubsubContext;
import com.google.cloud.pubsublite.internal.wire.RoutingMetadata;
import com.google.cloud.pubsublite.internal.wire.SubscriberBuilder;
import com.google.cloud.pubsublite.internal.wire.SubscriberFactory;
import com.google.cloud.pubsublite.proto.Cursor;
import com.google.cloud.pubsublite.proto.SeekRequest;
import com.google.cloud.pubsublite.v1.SubscriberServiceClient;
import com.google.cloud.pubsublite.v1.SubscriberServiceSettings;
import java.io.Serializable;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.util.function.SerializableSupplier;

@AutoValue
public abstract class PubsubLiteSourceSettings<OUT> implements Serializable {
  public static <OUT> Builder<OUT> builder() {
    return new AutoValue_PubsubLiteSourceSettings.Builder<OUT>()
        .setTimestampSelector(MessageTimestampExtractor.publishTimeExtractor())
        .setPartitionFinishedCondition(PartitionFinishedCondition.continueIndefinitely());
  }

  private static SubscriberServiceClient newSubscriberServiceClient(
      SubscriptionPath path, Partition partition) throws ApiException {
    try {
      SubscriberServiceSettings.Builder settingsBuilder = SubscriberServiceSettings.newBuilder();
      addDefaultMetadata(
          PubsubContext.of(PubsubContext.Framework.of("BEAM")),
          RoutingMetadata.of(path, partition),
          settingsBuilder);
      return SubscriberServiceClient.create(
          addDefaultSettings(path.location().region(), settingsBuilder));
    } catch (Throwable t) {
      throw toCanonical(t).underlying;
    }
  }

  public static SubscriberFactory getSubscriberFactory(
      SubscriptionPath path, Partition partition, SeekRequest seek) {
    return (consumer) ->
        SubscriberBuilder.newBuilder()
            .setSubscriptionPath(path)
            .setPartition(partition)
            .setServiceClient(newSubscriberServiceClient(path, partition))
            .setMessageConsumer(consumer)
            .setInitialLocation(seek)
            .build();
  }

  // Required
  public abstract SubscriptionPath subscriptionPath();

  public abstract PubsubLiteDeserializationSchema<OUT> deserializationSchema();

  public abstract FlowControlSettings flowControlSettings();

  // Optional
  public abstract MessageTimestampExtractor timestampSelector();

  public abstract PartitionFinishedCondition partitionFinishedCondition();

  // internal.
  abstract @Nullable SerializableSupplier<AdminClient> adminClientSupplier();

  abstract @Nullable SerializableSupplier<CursorClient> cursorClientSupplier();

  AdminClient getAdminClient() {
    if (adminClientSupplier() != null) {
      return adminClientSupplier().get();
    }
    return AdminClient.create(
        AdminClientSettings.newBuilder().setRegion(subscriptionPath().location().region()).build());
  }

  CursorClient getCursorClient() {
    if (cursorClientSupplier() != null) {
      return cursorClientSupplier().get();
    }
    return CursorClient.create(
        CursorClientSettings.newBuilder()
            .setRegion(subscriptionPath().location().region())
            .build());
  }

  CompletablePullSubscriber.Factory getSplitStateFactory() {
    return split -> {
      SeekRequest seek =
          SeekRequest.newBuilder()
              .setCursor(Cursor.newBuilder().setOffset(split.start().value()).build())
              .build();
      SubscriberFactory factory =
          getSubscriberFactory(split.subscriptionPath(), split.partition(), seek);

      BlockingPullSubscriber b = new BlockingPullSubscriberImpl(factory, flowControlSettings());
      return new CompletablePullSubscriberImpl(split, b, partitionFinishedCondition());
    };
  }

  Supplier<SplitReader<Record<OUT>, SubscriptionPartitionSplit>> getSplitReaderSupplier() {
    return () ->
        new DeserializingSplitReader<>(
            new MessageSplitReader(getSplitStateFactory()),
            deserializationSchema(),
            timestampSelector());
  }

  Consumer<SubscriptionPartitionSplit> getCursorCommitter() {
    CursorClient client = getCursorClient();
    return (SubscriptionPartitionSplit split) -> {
      client.commitCursor(split.subscriptionPath(), split.partition(), split.start());
    };
  }

  @AutoValue.Builder
  abstract static class Builder<OUT> {
    public abstract Builder<OUT> setSubscriptionPath(SubscriptionPath path);

    public abstract Builder<OUT> setDeserializationSchema(
        PubsubLiteDeserializationSchema<OUT> schema);

    public abstract Builder<OUT> setFlowControlSettings(FlowControlSettings settings);

    public abstract Builder<OUT> setTimestampSelector(MessageTimestampExtractor value);

    public abstract Builder<OUT> setPartitionFinishedCondition(PartitionFinishedCondition value);

    abstract Builder<OUT> setAdminClientSupplier(SerializableSupplier<AdminClient> value);

    abstract Builder<OUT> setCursorClientSupplier(SerializableSupplier<CursorClient> value);

    abstract PubsubLiteSourceSettings<OUT> build();
  }
}

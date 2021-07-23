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

import com.google.cloud.pubsublite.AdminClient;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.flink.enumerator.PartitionAssigner;
import com.google.cloud.pubsublite.flink.enumerator.PubsubLiteSplitEnumerator;
import com.google.cloud.pubsublite.flink.enumerator.SingleSubscriptionSplitDiscovery;
import com.google.cloud.pubsublite.flink.enumerator.SplitDiscovery;
import com.google.cloud.pubsublite.flink.enumerator.SplitEnumeratorCheckpointSerializer;
import com.google.cloud.pubsublite.flink.enumerator.UniformPartitionAssigner;
import com.google.cloud.pubsublite.flink.proto.SplitEnumeratorCheckpoint;
import com.google.cloud.pubsublite.flink.reader.PubsubLiteRecordEmitter;
import com.google.cloud.pubsublite.flink.reader.PubsubLiteSourceReader;
import com.google.cloud.pubsublite.flink.split.SubscriptionPartitionSplit;
import com.google.cloud.pubsublite.flink.split.SubscriptionPartitionSplitSerializer;
import com.google.cloud.pubsublite.internal.ExtractStatus;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.UserCodeClassLoader;

public class PubsubLiteSource<OUT>
    implements Source<OUT, SubscriptionPartitionSplit, SplitEnumeratorCheckpoint>,
        ResultTypeQueryable<OUT> {
  private final PubsubLiteSourceSettings<OUT> settings;

  public PubsubLiteSource(PubsubLiteSourceSettings<OUT> settings) {
    this.settings = settings;
  }

  @Override
  public Boundedness getBoundedness() {
    return null;
  }

  @Override
  public SourceReader<OUT, SubscriptionPartitionSplit> createReader(
      SourceReaderContext readerContext) throws Exception {
    PubsubLiteDeserializationSchema<OUT> schema = settings.deserializationSchema();
    schema.open(
        new DeserializationSchema.InitializationContext() {
          @Override
          public MetricGroup getMetricGroup() {
            return readerContext.metricGroup();
          }

          @Override
          public UserCodeClassLoader getUserCodeClassLoader() {
            return readerContext.getUserCodeClassLoader();
          }
        });
    return new PubsubLiteSourceReader<>(
        new PubsubLiteRecordEmitter<>(),
        settings.getCursorCommitter(),
        settings.getSplitReaderSupplier(),
        new Configuration(),
        readerContext);
  }

  @Override
  public SplitEnumerator<SubscriptionPartitionSplit, SplitEnumeratorCheckpoint> createEnumerator(
      SplitEnumeratorContext<SubscriptionPartitionSplit> enumContext) {
    TopicPath topic;
    try (AdminClient adminClient = settings.getAdminClient()) {
      topic =
          TopicPath.parse(
              adminClient.getSubscription(settings.subscriptionPath()).get().getTopic());
    } catch (Throwable t) {
      throw ExtractStatus.toCanonical(t).underlying;
    }
    return new PubsubLiteSplitEnumerator(
        enumContext,
        UniformPartitionAssigner.create(),
        SingleSubscriptionSplitDiscovery.create(
            settings.getAdminClient(),
            settings.getCursorClient(),
            topic,
            settings.subscriptionPath()),
        settings.boundedness());
  }

  @Override
  public SplitEnumerator<SubscriptionPartitionSplit, SplitEnumeratorCheckpoint> restoreEnumerator(
      SplitEnumeratorContext<SubscriptionPartitionSplit> enumContext,
      SplitEnumeratorCheckpoint checkpoint) {
    PartitionAssigner assigner =
        UniformPartitionAssigner.fromCheckpoint(checkpoint.getAssignmentsList());
    SplitDiscovery discovery =
        SingleSubscriptionSplitDiscovery.fromCheckpoint(
            checkpoint.getDiscovery(),
            assigner.listSplits(),
            settings.getAdminClient(),
            settings.getCursorClient());
    return new PubsubLiteSplitEnumerator(enumContext, assigner, discovery, settings.boundedness());
  }

  @Override
  public SimpleVersionedSerializer<SubscriptionPartitionSplit> getSplitSerializer() {
    return new SubscriptionPartitionSplitSerializer();
  }

  @Override
  public SimpleVersionedSerializer<SplitEnumeratorCheckpoint> getEnumeratorCheckpointSerializer() {
    return new SplitEnumeratorCheckpointSerializer();
  }

  @Override
  public TypeInformation<OUT> getProducedType() {
    return settings.deserializationSchema().getProducedType();
  }
}

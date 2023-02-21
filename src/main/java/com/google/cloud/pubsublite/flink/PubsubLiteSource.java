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

import com.google.cloud.pubsublite.flink.internal.source.SourceAssembler;
import com.google.cloud.pubsublite.flink.internal.source.enumerator.PartitionAssigner;
import com.google.cloud.pubsublite.flink.internal.source.enumerator.PubsubLiteSplitEnumerator;
import com.google.cloud.pubsublite.flink.internal.source.enumerator.SingleSubscriptionSplitDiscovery;
import com.google.cloud.pubsublite.flink.internal.source.enumerator.SplitDiscovery;
import com.google.cloud.pubsublite.flink.internal.source.enumerator.SplitEnumeratorCheckpointSerializer;
import com.google.cloud.pubsublite.flink.internal.source.enumerator.UniformPartitionAssigner;
import com.google.cloud.pubsublite.flink.internal.source.reader.CommitterCache;
import com.google.cloud.pubsublite.flink.internal.source.reader.PubsubLiteRecordEmitter;
import com.google.cloud.pubsublite.flink.internal.source.reader.PubsubLiteSourceReader;
import com.google.cloud.pubsublite.flink.internal.source.split.SubscriptionPartitionSplit;
import com.google.cloud.pubsublite.flink.internal.source.split.SubscriptionPartitionSplitSerializer;
import com.google.cloud.pubsublite.flink.proto.SplitEnumeratorCheckpoint;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.UserCodeClassLoader;

public class PubsubLiteSource<OutputT>
    implements Source<OutputT, SubscriptionPartitionSplit, SplitEnumeratorCheckpoint>,
        ResultTypeQueryable<OutputT> {
  private static final long serialVersionUID = 2304938420938L;
  private final PubsubLiteSourceSettings<OutputT> settings;

  public PubsubLiteSource(PubsubLiteSourceSettings<OutputT> settings) {
    this.settings = settings;
  }

  @Override
  public Boundedness getBoundedness() {
    return Boundedness.CONTINUOUS_UNBOUNDED;
  }

  @Override
  public SourceReader<OutputT, SubscriptionPartitionSplit> createReader(
      SourceReaderContext readerContext) throws Exception {
    PubsubLiteDeserializationSchema<OutputT> schema = settings.deserializationSchema();
    schema.open(
        new DeserializationSchema.InitializationContext() {
          @Override
          public MetricGroup getMetricGroup() {
            return readerContext.metricGroup();
          }

          @Override
          public UserCodeClassLoader getUserCodeClassLoader() {
            return null;
          }
        });
    SourceAssembler<OutputT> assembler = new SourceAssembler<>(settings);
    return new PubsubLiteSourceReader<>(
        new PubsubLiteRecordEmitter<>(),
        assembler.getSplitReaderSupplier(),
        new Configuration(),
        readerContext,
        new CommitterCache(assembler::getCommitter));
  }

  @Override
  public SplitEnumerator<SubscriptionPartitionSplit, SplitEnumeratorCheckpoint> createEnumerator(
      SplitEnumeratorContext<SubscriptionPartitionSplit> enumContext) {
    SourceAssembler<OutputT> assembler = new SourceAssembler<>(settings);
    return new PubsubLiteSplitEnumerator(
        enumContext,
        UniformPartitionAssigner.create(),
        SingleSubscriptionSplitDiscovery.create(
            assembler.getUnownedAdminClient(),
            assembler.getUnownedCursorClient(),
            assembler.getTopicPath(),
            settings.subscriptionPath()));
  }

  @Override
  public SplitEnumerator<SubscriptionPartitionSplit, SplitEnumeratorCheckpoint> restoreEnumerator(
      SplitEnumeratorContext<SubscriptionPartitionSplit> enumContext,
      SplitEnumeratorCheckpoint checkpoint) {
    PartitionAssigner assigner =
        UniformPartitionAssigner.fromCheckpoint(checkpoint.getAssignmentsList());
    SourceAssembler<OutputT> assembler = new SourceAssembler<>(settings);
    SplitDiscovery discovery =
        SingleSubscriptionSplitDiscovery.fromCheckpoint(
            checkpoint.getDiscovery(),
            assigner.listSplits(),
            assembler.getUnownedAdminClient(),
            assembler.getUnownedCursorClient());
    return new PubsubLiteSplitEnumerator(enumContext, assigner, discovery);
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
  public TypeInformation<OutputT> getProducedType() {
    return settings.deserializationSchema().getProducedType();
  }
}

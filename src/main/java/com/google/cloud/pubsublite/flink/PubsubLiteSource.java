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
import com.google.cloud.pubsublite.flink.enumerator.PubsubLiteSplitEnumerator;
import com.google.cloud.pubsublite.flink.enumerator.PubsubLiteSplitEnumeratorProtoSerializer;
import com.google.cloud.pubsublite.flink.proto.PubsubLiteSplitEnumeratorProto;
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
    implements Source<OUT, SubscriptionPartitionSplit, PubsubLiteSplitEnumeratorProto>,
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
        settings.getCursorCommitter(),
        settings.getSplitReaderSupplier(),
        new Configuration(),
        readerContext);
  }

  @Override
  public SplitEnumerator<SubscriptionPartitionSplit, PubsubLiteSplitEnumeratorProto>
      createEnumerator(SplitEnumeratorContext<SubscriptionPartitionSplit> enumContext) {
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
        topic,
        settings.subscriptionPath(),
        settings.getAdminClient(),
        settings.getCursorClient(),
        Boundedness.CONTINUOUS_UNBOUNDED);
  }

  @Override
  public SplitEnumerator<SubscriptionPartitionSplit, PubsubLiteSplitEnumeratorProto>
      restoreEnumerator(
          SplitEnumeratorContext<SubscriptionPartitionSplit> enumContext,
          PubsubLiteSplitEnumeratorProto checkpoint) {
    return new PubsubLiteSplitEnumerator(
        enumContext,
        checkpoint,
        settings.getAdminClient(),
        settings.getCursorClient(),
        Boundedness.CONTINUOUS_UNBOUNDED);
  }

  @Override
  public SimpleVersionedSerializer<SubscriptionPartitionSplit> getSplitSerializer() {
    return new SubscriptionPartitionSplitSerializer();
  }

  @Override
  public SimpleVersionedSerializer<PubsubLiteSplitEnumeratorProto>
      getEnumeratorCheckpointSerializer() {
    return new PubsubLiteSplitEnumeratorProtoSerializer();
  }

  @Override
  public TypeInformation<OUT> getProducedType() {
    return settings.deserializationSchema().getProducedType();
  }
}

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

import com.google.cloud.Tuple;
import com.google.cloud.pubsublite.flink.sink.BulkWaitPublisher;
import com.google.cloud.pubsublite.flink.sink.MessagePublisher;
import com.google.cloud.pubsublite.flink.sink.PerServerPublisherCache;
import com.google.cloud.pubsublite.flink.sink.SerializingPublisher;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import java.time.Instant;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class PubsubLiteSink<T> extends RichSinkFunction<T> implements CheckpointedFunction {
  private final PubsubLiteSinkSettings<T> settings;

  @GuardedBy("this")
  private transient BulkWaitPublisher<Tuple<T, Instant>> publisher;

  public PubsubLiteSink(PubsubLiteSinkSettings<T> settings) {
    this.settings = settings;
  }

  @Override
  public synchronized void snapshotState(
      org.apache.flink.runtime.state.FunctionSnapshotContext functionSnapshotContext)
      throws Exception {
    publisher.waitUntilNoOutstandingPublishes();
  }

  @Override
  public void initializeState(
      org.apache.flink.runtime.state.FunctionInitializationContext functionInitializationContext)
      throws Exception {}

  @Override
  public synchronized void invoke(T value, Context context) throws Exception {
    publisher.publish(Tuple.of(value, Instant.ofEpochMilli(context.timestamp())));
  }

  @Override
  public synchronized void open(Configuration parameters) throws Exception {
    super.open(parameters);
    publisher =
        new SerializingPublisher<>(
            new MessagePublisher(
                PerServerPublisherCache.getOrCreate(settings.getPublisherConfig())),
            settings.serializationSchema());
  }
}

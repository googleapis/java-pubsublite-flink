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
import com.google.cloud.pubsublite.flink.internal.sink.BulkWaitPublisher;
import com.google.cloud.pubsublite.flink.internal.sink.MessagePublisher;
import com.google.cloud.pubsublite.flink.internal.sink.PerServerPublisherCache;
import com.google.cloud.pubsublite.flink.internal.sink.SerializingPublisher;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import java.time.Instant;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class PubsubLiteSink<T> extends RichSinkFunction<T> implements CheckpointedFunction {
  private static final long serialVersionUID = 849752028745098L;

  private final PubsubLiteSinkSettings<T> settings;

  @GuardedBy("this")
  private transient BulkWaitPublisher<Tuple<T, Instant>> publisher;

  public PubsubLiteSink(PubsubLiteSinkSettings<T> settings) {
    this.settings = settings;
  }

  @Override
  public void initializeState(FunctionInitializationContext functionInitializationContext)
      throws Exception {}

  @Override
  public synchronized void snapshotState(FunctionSnapshotContext functionSnapshotContext)
      throws Exception {
    publisher.waitUntilNoOutstandingPublishes();
  }

  @Override
  public synchronized void invoke(T value, Context context) throws Exception {
    Long timestamp = context.timestamp();
    if (timestamp == null) {
      timestamp = context.currentProcessingTime();
    }
    publisher.publish(Tuple.of(value, Instant.ofEpochMilli(timestamp)));
  }

  @Override
  public synchronized void open(Configuration parameters) throws Exception {
    super.open(parameters);
    publisher =
        new SerializingPublisher<>(
            new MessagePublisher(
                PerServerPublisherCache.getOrCreate(settings), settings.maxBytesOutstanding()),
            settings.serializationSchema());
  }

  @Override
  public synchronized void close() throws Exception {
    publisher.waitUntilNoOutstandingPublishes();
  }
}

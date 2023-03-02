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

import com.google.cloud.pubsublite.flink.internal.sink.MessagePublisher;
import com.google.cloud.pubsublite.flink.internal.sink.PerServerPublisherCache;
import com.google.cloud.pubsublite.flink.internal.sink.PubsubLiteSinkWriter;
import java.io.IOException;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

public class PubsubLiteSink<T> implements Sink<T> {
  private static final long serialVersionUID = 849752028745098L;

  private final PubsubLiteSinkSettings<T> settings;

  public PubsubLiteSink(PubsubLiteSinkSettings<T> settings) {
    this.settings = settings;
  }

  @Override
  public SinkWriter<T> createWriter(InitContext initContext) throws IOException {
    PubsubLiteSerializationSchema<T> schema = settings.serializationSchema();
    try {
      schema.open(initContext.asSerializationSchemaInitializationContext());
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
    return new PubsubLiteSinkWriter<>(
        new MessagePublisher(
            PerServerPublisherCache.getOrCreate(settings), settings.maxBytesOutstanding()),
        schema);
  }
}

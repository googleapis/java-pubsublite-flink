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
package com.google.cloud.pubsublite.flink.samples;

import com.google.cloud.pubsublite.Message;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.flink.PubsubLiteSink;
import com.google.cloud.pubsublite.flink.PubsubLiteSinkSettings;
import com.google.cloud.pubsublite.proto.PubSubMessage;
import com.google.protobuf.ByteString;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SimpleWrite {

  public static void main(String[] args) throws Exception {
    ParameterTool parameter = ParameterTool.fromArgs(args);

    PubsubLiteSinkSettings<Message> settings =
        PubsubLiteSinkSettings.messagesBuilder()
            .setTopicPath(TopicPath.parse(parameter.get("topic")))
            .setMaxBytesOutstanding(1000)
            .build();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.fromSequence(0, 999)
        .map(
            s ->
                Message.fromProto(
                    PubSubMessage.newBuilder()
                        .setData(ByteString.copyFromUtf8("message " + s))
                        .build()))
        .addSink(new PubsubLiteSink<>(settings));
    env.execute();
  }
}

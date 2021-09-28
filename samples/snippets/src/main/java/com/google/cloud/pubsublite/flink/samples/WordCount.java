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

import com.google.cloud.pubsublite.SequencedMessage;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.cloudpubsub.FlowControlSettings;
import com.google.cloud.pubsublite.flink.PubsubLiteSource;
import com.google.cloud.pubsublite.flink.PubsubLiteSourceSettings;
import com.google.cloud.pubsublite.flink.StopCondition;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.util.Collector;

public class WordCount {

  public static void main(String[] args) throws Exception {
    ParameterTool parameter = ParameterTool.fromArgs(args);

    PubsubLiteSourceSettings<SequencedMessage> settings =
        PubsubLiteSourceSettings.messagesBuilder()
            .setFlowControlSettings(
                FlowControlSettings.builder()
                    .setBytesOutstanding(1000L)
                    .setMessagesOutstanding(1000L)
                    .build())
            .setSubscriptionPath(SubscriptionPath.parse(parameter.get("subscription")))
            .setBoundedness(Boundedness.BOUNDED)
            .setStopCondition(StopCondition.readToHead())
            .build();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.fromSource(new PubsubLiteSource<>(settings), WatermarkStrategy.noWatermarks(), "Source")
        .map(m -> m.message().data().toStringUtf8())
        .flatMap(new LineSplitter())
        .keyBy(v -> v.f0)
        .sum(1)
        .addSink(new PrintSinkFunction<>());
    env.execute();
  }

  public static class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
    @Override
    public void flatMap(String line, Collector<Tuple2<String, Integer>> out) {
      line =
          line.replaceAll("[:;,.!]", "")
              .replaceAll("\n", " ")
              .replaceAll("\\s+", " ")
              .toLowerCase();
      for (String word : line.split(" ")) {
        if (word.length() > 0) {
          out.collect(Tuple2.of(word, 1));
        }
      }
    }
  }
}

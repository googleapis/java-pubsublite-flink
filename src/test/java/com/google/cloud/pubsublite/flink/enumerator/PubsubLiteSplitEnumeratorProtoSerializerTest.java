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
package com.google.cloud.pubsublite.flink.enumerator;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.pubsublite.flink.proto.PubsubLiteSplitEnumeratorProto;
import java.io.IOException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PubsubLiteSplitEnumeratorProtoSerializerTest {

  @Test
  public void testSerialization() throws IOException {
    PubsubLiteSplitEnumeratorProto proto =
        PubsubLiteSplitEnumeratorProto.newBuilder()
            .setDiscovery(
                PubsubLiteSplitEnumeratorProto.Discovery.newBuilder().setSubscription("sub"))
            .build();
    PubsubLiteSplitEnumeratorProtoSerializer serializer =
        new PubsubLiteSplitEnumeratorProtoSerializer();
    assertThat(serializer.deserialize(serializer.getVersion(), serializer.serialize(proto)))
        .isEqualTo(proto);
  }
}

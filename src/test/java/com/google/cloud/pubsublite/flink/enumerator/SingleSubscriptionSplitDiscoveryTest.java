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

import static com.google.cloud.pubsublite.internal.testing.UnitTestExamples.exampleSubscriptionPath;
import static com.google.cloud.pubsublite.internal.testing.UnitTestExamples.exampleTopicPath;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.pubsublite.AdminClient;
import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.flink.proto.SplitEnumeratorCheckpoint;
import com.google.cloud.pubsublite.flink.split.SubscriptionPartitionSplit;
import com.google.cloud.pubsublite.internal.CheckedApiException;
import com.google.cloud.pubsublite.internal.CursorClient;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SingleSubscriptionSplitDiscoveryTest {

  @Mock CursorClient mockCursorClient;
  @Mock AdminClient mockAdminClient;

  SplitDiscovery discovery;

  @Before
  public void setUp() {
    discovery =
        SingleSubscriptionSplitDiscovery.create(
            mockAdminClient, mockCursorClient, exampleTopicPath(), exampleSubscriptionPath());
  }

  @Test
  public void testDiscovery() {
    when(mockAdminClient.getTopicPartitionCount(exampleTopicPath()))
        .thenReturn(ApiFutures.immediateFuture(2L));
    when(mockCursorClient.listPartitionCursors(exampleSubscriptionPath()))
        .thenReturn(ApiFutures.immediateFuture(ImmutableMap.of(Partition.of(1), Offset.of(2))));
    List<SubscriptionPartitionSplit> splits = discovery.discoverSplits();
    assertThat(splits)
        .containsExactly(
            SubscriptionPartitionSplit.create(
                exampleSubscriptionPath(), Partition.of(0), Offset.of(0)),
            SubscriptionPartitionSplit.create(
                exampleSubscriptionPath(), Partition.of(1), Offset.of(2)));
  }

  @Test
  public void testDiscovery_Incremental() {
    when(mockAdminClient.getTopicPartitionCount(exampleTopicPath()))
        .thenReturn(ApiFutures.immediateFuture(2L))
        .thenReturn(ApiFutures.immediateFuture(3L))
        .thenReturn(ApiFutures.immediateFuture(3L));
    when(mockCursorClient.listPartitionCursors(exampleSubscriptionPath()))
        .thenReturn(ApiFutures.immediateFuture(ImmutableMap.of(Partition.of(1), Offset.of(2))));
    assertThat(discovery.discoverSplits()).hasSize(2);
    assertThat(discovery.discoverSplits()).hasSize(1);
    assertThat(discovery.discoverSplits()).hasSize(0);
  }

  @Test
  public void testDiscovery_AdminFailure() {
    when(mockAdminClient.getTopicPartitionCount(exampleTopicPath()))
        .thenReturn(
            ApiFutures.immediateFailedFuture(
                new CheckedApiException("", StatusCode.Code.INTERNAL)));
    assertThrows(ApiException.class, () -> discovery.discoverSplits());
  }

  @Test
  public void testDiscovery_CursorFailure() {
    when(mockAdminClient.getTopicPartitionCount(exampleTopicPath()))
        .thenReturn(ApiFutures.immediateFuture(2L));
    when(mockCursorClient.listPartitionCursors(exampleSubscriptionPath()))
        .thenReturn(
            ApiFutures.immediateFailedFuture(
                new CheckedApiException("", StatusCode.Code.INTERNAL)));
    assertThrows(ApiException.class, () -> discovery.discoverSplits());
  }

  @Test
  public void testCheckpoint() {
    SplitEnumeratorCheckpoint.Discovery proto = discovery.checkpoint();
    assertThat(proto.getSubscription()).isEqualTo(exampleSubscriptionPath().toString());
    assertThat(proto.getTopic()).isEqualTo(exampleTopicPath().toString());
  }

  @Test
  public void testCheckpointRestore() {
    SplitEnumeratorCheckpoint.Discovery proto = discovery.checkpoint();

    List<SubscriptionPartitionSplit> splits =
        ImmutableList.of(
            SubscriptionPartitionSplit.create(
                exampleSubscriptionPath(), Partition.of(2), Offset.of(4)));
    SplitDiscovery restored =
        SingleSubscriptionSplitDiscovery.fromCheckpoint(
            proto, splits, mockAdminClient, mockCursorClient);

    when(mockAdminClient.getTopicPartitionCount(exampleTopicPath()))
        .thenReturn(ApiFutures.immediateFuture(4L));
    assertThat(restored.discoverSplits()).hasSize(1);
  }

  @Test
  public void testCheckpointRestore_SubscriptionMismatch() {
    SplitEnumeratorCheckpoint.Discovery proto = discovery.checkpoint();

    List<SubscriptionPartitionSplit> splits =
        ImmutableList.of(
            SubscriptionPartitionSplit.create(
                SubscriptionPath.parse(exampleSubscriptionPath().toString() + "-other"), Partition.of(2), Offset.of(4)));
    assertThrows(IllegalStateException.class, () -> {
      SingleSubscriptionSplitDiscovery.fromCheckpoint(
          proto, splits, mockAdminClient, mockCursorClient);
    });
  }

  @Test
  public void testClose() throws Exception {
    discovery.close();
    verify(mockAdminClient).close();
    verify(mockCursorClient).close();
  }
}

package com.google.cloud.pubsublite.flink.sink;

import static com.google.cloud.pubsublite.flink.TestUtilities.messageFromOffset;
import static com.google.cloud.pubsublite.flink.reader.ReaderUtils.recordWithSplitsToMap;
import static com.google.cloud.pubsublite.internal.testing.UnitTestExamples.examplePartition;
import static com.google.cloud.pubsublite.internal.testing.UnitTestExamples.exampleSubscriptionPath;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MessagePublisherTest {

}
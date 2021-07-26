package com.google.cloud.pubsublite.flink.sink;

import com.google.cloud.pubsublite.internal.CheckedApiException;

public interface AtLeastOncePublisher<T> {

  void publish(T message) throws CheckedApiException;

  void waitUntilNoOutstandingPublishes() throws CheckedApiException, InterruptedException;
}

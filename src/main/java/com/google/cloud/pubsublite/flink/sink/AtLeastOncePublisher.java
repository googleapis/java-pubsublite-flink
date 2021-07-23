package com.google.cloud.pubsublite.flink.sink;

import com.google.cloud.pubsublite.internal.CheckedApiException;

public interface AtLeastOncePublisher<T> extends AutoCloseable {

  void publish(T message) throws CheckedApiException;

  void checkpoint() throws CheckedApiException, InterruptedException;
}

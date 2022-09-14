package com.htec.tasks.first;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

public abstract class AbstractTaskExecutorFactory<T> {
  abstract AbstractTaskExecutor getAbstractTaskExecutor(
      Consumer<T> successfulCallback, BiConsumer<T, Exception> failCallback);
}

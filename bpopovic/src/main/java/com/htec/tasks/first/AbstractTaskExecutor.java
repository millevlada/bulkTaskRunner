package com.htec.tasks.first;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

public abstract class AbstractTaskExecutor<T extends Runnable> extends Thread {
  Consumer<T> successfulCallback;
  BiConsumer<T, Exception> failCallback;
  T task;

  protected AbstractTaskExecutor(
      Consumer<T> successfulCallback, BiConsumer<T, Exception> failCallback) {
    this.successfulCallback = successfulCallback;
    this.failCallback = failCallback;
  }

  public void addTask(T task) {
    this.task = task;
  }

  @Override
  public void run() {
    try {
      task.run();
      successfulCallback.accept(task);
    } catch (Exception e) {
      failCallback.accept(task, e);
    }
  }
}

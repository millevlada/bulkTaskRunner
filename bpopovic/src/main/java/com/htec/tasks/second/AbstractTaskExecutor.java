package com.htec.tasks.second;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

public abstract class AbstractTaskExecutor<T extends Runnable> extends Thread {
  Consumer<T> successfulCallback;
  BiConsumer<T, Exception> failCallback;
  T task;
  private static final int MAX_RETRY = 5;
  private boolean success = false;

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
    int numberOfRuns = 1;

    while (numberOfRuns <= MAX_RETRY && !success) {
      try {
        task.run();
        success = true;
        successfulCallback.accept(task);
      } catch (Exception e) {
        numberOfRuns++;
        if (numberOfRuns == MAX_RETRY) {
          failCallback.accept(task, e);
        }
      }
    }
  }

  public boolean getSuccessStatus(){
    return success;
  }
}

package com.htec.tasks.first;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class BulkTaskRunner<T extends Runnable> implements AutoCloseable {

  private AbstractTaskExecutorFactory abstractTaskExecutorFactory;
  private List<AbstractTaskExecutor> availableThreads;
  private List<AbstractTaskExecutor> workingThreads;

  private Consumer<T> successfulCallback;
  private BiConsumer<T, Exception> failCallback;
  private int numberOfThreads;
  private boolean isBlocked = false;

  public void start(
      int numberOfThreads, Consumer<T> successfulCallback, BiConsumer<T, Exception> failCallback) {
    this.successfulCallback = successfulCallback;
    this.failCallback = failCallback;
    this.numberOfThreads = numberOfThreads;
    for (int i = 0; i < numberOfThreads; i++) {
      availableThreads.add(
          abstractTaskExecutorFactory.getAbstractTaskExecutor(successfulCallback, failCallback));
    }
  }

  public synchronized void addTask(T task) {
    while (true) {
      if (availableThreads.size() + workingThreads.size() < numberOfThreads) {
        availableThreads.add(
            abstractTaskExecutorFactory.getAbstractTaskExecutor(successfulCallback, failCallback));
        continue;
      }
      if (availableThreads.size() > 0 && !isBlocked) {
        AbstractTaskExecutor thread = availableThreads.remove(0);
        thread.addTask(task);
        thread.start();
        workingThreads.add(thread);
        break;
      } else {
        try {
          wait(1000);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  public synchronized void waitTillFinished() {
    isBlocked = true;
  }

  public int getNumberOfRunningExecutors() {
    return workingThreads.size();
  }

  @Override
  public void close() throws Exception {
    for (AbstractTaskExecutor thread : availableThreads) {
      thread.interrupt();
    }

    for (AbstractTaskExecutor thread : workingThreads) {
      thread.interrupt();
    }
  }
}

package com.htec.examples.parallelrunner.client.task;

import com.htec.util.Progress;
import org.eclipse.jdt.annotation.NonNullByDefault;

@NonNullByDefault
/**
 * Data task indices contract.
 */
public interface IDataTask<T> {

    T getInputData();

    String getAuditId();

    void run(Progress progress) throws Exception;

    void setOnCompleted(Runnable onSuccess);
}

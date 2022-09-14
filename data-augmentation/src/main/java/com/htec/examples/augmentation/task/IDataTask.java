package com.htec.examples.augmentation.task;

import com.htec.util.Progress;
import org.eclipse.jdt.annotation.NonNullByDefault;

/**
 * Data task indices contract.
 */
@NonNullByDefault
public interface IDataTask<T> {

    String getAuditId();

    void run(Progress progress) throws Exception;

    void setOnCompleted(Runnable onSuccess);
}

package com.htec.examples.augmentation.task;

import com.htec.util.JdbcAnyRecord;
import com.htec.util.Progress;
import org.eclipse.jdt.annotation.NonNullByDefault;
import org.eclipse.jdt.annotation.Nullable;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Simulates some service that augments given data.
 * For purpose of example, we introduce factor of instability -> 1 of every {@link AddHashColumnTask#ERROR_PROBABILITY_FACTOR}
 * tasks would fail with temporary unavailable exception. This kind of errors should be retired.
 * For the purpose of the example, we have chance of having combination of data that gives irreparable error too. This
 * kind of error is not retired since it represents kind of invalid data for our stub augmentation task.
 */
@NonNullByDefault
public class AddHashColumnTask implements IDataTask<JdbcAnyRecord> {

    private final int ERROR_PROBABILITY_FACTOR = 100;

    private final JdbcAnyRecord m_data;

    @Nullable
    private Runnable m_onSuccess;

    public AddHashColumnTask(JdbcAnyRecord data) {
        m_data = data;
    }

    @Override
    public String getAuditId() {
        Object id = m_data.get("ID");
        return null == id
           ? "null"
           : id.toString();
    }

    @Override
    public void run(Progress p) throws Exception {
        if(isIrreparableError(m_data)) {
            throw new Exception("Your data contains [Irreparable Error]!");
        }
        if(ThreadLocalRandom.current().nextInt(ERROR_PROBABILITY_FACTOR) == 0) {
            throw new Exception("bad luck, service is currently unavailable");
        }
        int hash = 0;
        for(String col: m_data.getAllColumnNames()) {
            Object o = m_data.get(col);
            hash += null != o
                ? o.hashCode()
                : 0;
        }
        //as a simulation of some data augmentation we alter data value, by adding hash value to column hash
        m_data.set("hash", hash);

        Runnable onSuccess = m_onSuccess;
        if(null != onSuccess) {
            onSuccess.run();
        }
    }

    @Override
    public void setOnCompleted(Runnable callback) {
        m_onSuccess = callback;
    }


    /**
     * For the purpose of simulation, we define certain combination of data as irreparable error.
     */
    public static boolean isIrreparableError(JdbcAnyRecord rec) {
        return "Fred".equals(rec.get("col1")) && "hates".equals(rec.get("col2")) && "Apple".equals(rec.get("col3"));
    }
}

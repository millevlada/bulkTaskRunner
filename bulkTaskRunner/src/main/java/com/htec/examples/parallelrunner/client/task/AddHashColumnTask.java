package com.htec.examples.parallelrunner.client.task;

import com.htec.examples.parallelrunner.client.data.FakeResultSet;
import com.htec.util.JdbcAnyRecord;
import com.htec.util.Progress;
import org.eclipse.jdt.annotation.NonNullByDefault;
import org.eclipse.jdt.annotation.Nullable;

import java.util.concurrent.ThreadLocalRandom;

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
    public JdbcAnyRecord getInputData() {
        return m_data;
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
        if(FakeResultSet.isIrreparableError(m_data)) {
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
}

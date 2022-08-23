package com.htec.examples.parallelrunner.client;

import com.htec.examples.parallelrunner.bulk.BulkTaskRunner;
import com.htec.examples.parallelrunner.client.AugmentBulkDataExecutor.FailedAugmentationRecord;
import com.htec.examples.parallelrunner.client.data.AugmentBulkData;
import com.htec.examples.parallelrunner.client.data.FakeResultSet;
import com.htec.util.*;
import org.eclipse.jdt.annotation.NonNull;
import org.eclipse.jdt.annotation.NonNullByDefault;
import org.eclipse.jdt.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

@NonNullByDefault
public class AugmentDataLoader {

    private static final int AUGMENT_CHUNK_SIZE = 100;

    private static final int TOTAL_RECORDS_FOR_AUGMENT = 1000;

    private final int m_maxThreads = 8;

    private int m_numberOfFoundRecordsForAugment = -1;

    private int m_maxRepeatsOnFailure = -1;

    private List<FailedAugmentationRecord> m_allRetriesFailedRecordsList = new ArrayList<>();

    private List<FailedAugmentationRecord> m_irreparableRecordsList = new ArrayList<>();

    private AtomicInteger m_numberOfAugmentedRecords = new AtomicInteger(0);

    @Nullable
    private BulkTaskRunner<AugmentBulkData> m_runner;

    private final List<AugmentBulkData> m_reworkList = new ArrayList<>();

    public void run() throws Exception {

        String inputQuery = "SELECT col1, col2, ... FROM table1 WHERE hashCols is null"; //hypothetical query that returns data to augment
        m_numberOfFoundRecordsForAugment = getTotalNumberOfRecordsForAugment(inputQuery);
        Progress rootProgress = setupProgress();
        m_maxRepeatsOnFailure = 5;
        rootProgress.addListener(new IProgressListener() {
            @Override
            public void progressed(@NonNull Progress level) throws Exception {
                String msg = rootProgress.getPercentage() + "%";
                BulkTaskRunner<AugmentBulkData> runner = m_runner;
                if(null != runner) {
                    msg += " busy with " + runner.getNumberOfRunningExecutors() + " threads";
                    System.out.println(msg);
                }
            }
        });

        if(m_numberOfFoundRecordsForAugment > 0) {
            //PreparedStatement ps = null;
            //ResultSet rs = null;
            try {
                //ps = conn.prepareStatement(inputQuery);
                //rs = ps.executeQuery();
                FakeResultSet rs = new FakeResultSet(m_numberOfFoundRecordsForAugment);

                doAugmentationWhileThereIsWork(rootProgress, rs);
            } finally {
                //FileTool.closeAll(rs, ps);
            }
        }

        BulkTaskRunner<AugmentBulkData> runner = m_runner;
        if(null != runner) {
            //if no records were scheduled, runner is not initialized
            runner.waitTillFinished();
            runner.close();
            m_runner = null;
        }
        requireNonNull(rootProgress).complete();

        //-- Report summary
        System.out.println("=============== Augment Data report =====================");
        reportErrors();
        System.out.println("Total of " + m_numberOfFoundRecordsForAugment + " records were tried to be augmented");
        System.out.println("Total of " + m_numberOfAugmentedRecords + " records have received data changes");
    }

    private void doAugmentationWhileThereIsWork(Progress p, FakeResultSet rs) throws Exception {
        List<JdbcAnyRecord> chunk = new ArrayList<>();
        boolean hadAnyWork = false;

        //first we do chunks from selected result set
        while(rs.hasNext()) {
            JdbcAnyRecord a = rs.next();
            chunk.add(a);
            hadAnyWork = true;
            if(chunk.size() == AUGMENT_CHUNK_SIZE) {
                runAugmentRecordsOnce(p, new AugmentBulkData(chunk, 0));
                chunk = new ArrayList<>();
            }
        }
        if(!chunk.isEmpty()) {
            runAugmentRecordsOnce(p, new AugmentBulkData(chunk, 0));
        }

        if(!hadAnyWork) {
            return;
        }

        //after all initial chunks are sent, we loop while there is any rework left
        long warntime = System.currentTimeMillis() + 60 * 1000;                // Wait for one minute before reporting loop state
        BulkTaskRunner<AugmentBulkData> runner = requireNonNull(m_runner);
        for(; ; ) {
            long cts = System.currentTimeMillis();
            boolean report = cts > warntime;
            AugmentBulkData rework = null;
            synchronized(m_reworkList) {
                if(report) {
                    warntime = cts + 60 * 1000;
                }

                if(!m_reworkList.isEmpty()) {
                    rework = m_reworkList.remove(0);
                }
            }
            if(null != rework) {
                if(report)
                    System.out.println("dbg: rework present, " + rework.getRecords().size() + " records need a retry");
                runAugmentRecordsOnce(p, rework);
            } else {
                int numberOfRunningExecutors = runner.getNumberOfRunningExecutors();
                if(numberOfRunningExecutors == 0) {
                    break;
                } else {
                    if(report) {
                        System.out.println("dbg: there are still " + numberOfRunningExecutors + " running, sleeping");
                        runner.reportStatus();
                    }
                    try {
                        Thread.sleep(1_000);
                    } catch(InterruptedException ex) {
                        ex.printStackTrace();
                    }
                }
            }
        }
    }

    private void runAugmentRecordsOnce(Progress p, AugmentBulkData data) throws Exception {
        System.out.println("Scheduling bulk of data " + data.getRecords().size());
        BulkTaskRunner<AugmentBulkData> runner = m_runner;
        if(runner == null) {
            runner = m_runner = new BulkTaskRunner<>();

            runner.start(me -> new AugmentBulkDataExecutor(p, me),
                    m_maxThreads,
                    executor -> {
                        AugmentBulkDataExecutor abdExecutor = (AugmentBulkDataExecutor) executor;
                        m_numberOfAugmentedRecords.getAndAdd(abdExecutor.getNumberOfAugmentedRecords());
                        List<FailedAugmentationRecord> failures = abdExecutor.getFailures();
                        int retryIndex = abdExecutor.getCurrentTaskRetryIndex();
                        if(!failures.isEmpty()) {
                            try {
                                m_onFailedCallback.accept(failures, retryIndex);
                            } catch(Exception ex) {
                                throw new WrappedException("Failure inside onFailedCallback", ex);
                            }
                        }
                    },
                    (executor, ex) -> {
                        throw new WrappedException("Exception during data augmentation", ex);
                    }
            );
        }

        //m_logger.log("Starting to augment chunk of next " + data.getRecords().size() + " records in target table");

        runner.addTask(data);
    }

    private final BiConsumerEx<List<FailedAugmentationRecord>, Integer> m_onFailedCallback = (failedRecords, retryIndex) -> {

        List<FailedAugmentationRecord> reparableFailedActions = scanForReparableFailures(failedRecords);

        if(reparableFailedActions.isEmpty()) {
            return;
        }

        Integer maxRepeatsOnFailure = m_maxRepeatsOnFailure;

        //-- We have failures. Retry if that is specified, else report and continue.
        if(null == maxRepeatsOnFailure || maxRepeatsOnFailure == 0) {
            System.err.println("Reparable " + reparableFailedActions.size() + " error(s) in augment data, no retries defined, adding them to failed items and continuing...");
            return;
        }

        if(retryIndex > maxRepeatsOnFailure) {
            //-- All attempts failed.
            System.err.println("All configured retries (" + maxRepeatsOnFailure + ") failed: we still have " + reparableFailedActions.size()
                    + " row(s) that did not process, adding them to failed items and continuing...");
            m_allRetriesFailedRecordsList.addAll(reparableFailedActions);
            return;
        }
        //m_logger.log(reparableFailedActions.size() + " repairable error(s) - retrying " + retryIndex + "/ of total retries " + maxRepeatsOnFailure);

        List<JdbcAnyRecord> failItemList = reparableFailedActions.stream()
                .map(a -> a.getRecord())
                .collect(Collectors.toList());

        addRework(new AugmentBulkData(failItemList, retryIndex + 1));
    };

    private void addRework(AugmentBulkData rework) {
        synchronized (m_reworkList) {
            m_reworkList.add(rework);
        }
    }

    @NonNull
    private List<FailedAugmentationRecord> scanForReparableFailures(List<FailedAugmentationRecord> failedRecords) {
        List<FailedAugmentationRecord> reparableFailedActions = new ArrayList<>();
        for(FailedAugmentationRecord failedRecord : failedRecords) {
            Exception exception = failedRecord.getException();
            if(isIrreparableException(exception)) {
                m_irreparableRecordsList.add(failedRecord);
            } else {
                reparableFailedActions.add(failedRecord);
            }
        }
        return reparableFailedActions;
    }

    private int reportErrors() {
        int errors = 0;
        if(!m_irreparableRecordsList.isEmpty()) {
            System.out.println("Total of " + m_irreparableRecordsList.size() + " records had irreparable failures");
            for(FailedAugmentationRecord irrFailed : m_irreparableRecordsList) {
                String errorMessage = irrFailed.getException().getLocalizedMessage();
                errors++;
                System.out.println("Failed " + showDataFor(irrFailed.getRecord()) + ": " + errorMessage);
            }
        }
        if(!m_allRetriesFailedRecordsList.isEmpty()) {
            System.out.println("Total of " + m_allRetriesFailedRecordsList.size() + " records had reparable failures with all retries failed");
            for(FailedAugmentationRecord irrFailed : m_allRetriesFailedRecordsList) {
                String errorMessage = irrFailed.getException().getLocalizedMessage();
                errors++;
                System.out.println("Failed " + showDataFor(irrFailed.getRecord()) + ": " + errorMessage);
            }
        }
        if(errors > 0) {
            System.err.println("Total " + errors + " errors ^^^ in augmenting data!");
        }
        return errors;
    }

    private String showDataFor(JdbcAnyRecord record) {
        String[] cols = {"col1", "col2", "col3"};
        StringBuilder sb = new StringBuilder();
        for(String colName: cols) {
            if(sb.length() > 0) {
                sb.append(", ");
            }
            sb.append(colName).append("=").append(record.get(colName));
        }
        return sb.toString();
    }

    private boolean isIrreparableException(Exception exception) {
        return exception.getMessage().contains("[Irreparable Error]");
    }

    private int getTotalNumberOfRecordsForAugment(String inputQuery) {
        return TOTAL_RECORDS_FOR_AUGMENT;
    }

    private Progress setupProgress() {
        Progress p = new Progress("root");
        p.setTotalWork(m_numberOfFoundRecordsForAugment);
        return p;
    }

    public static void main(String[] args) throws Exception {
        AugmentDataLoader adl = new AugmentDataLoader();
        adl.run();
    }
}

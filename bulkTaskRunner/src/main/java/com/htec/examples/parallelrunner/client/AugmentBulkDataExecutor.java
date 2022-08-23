package com.htec.examples.parallelrunner.client;

import com.htec.examples.parallelrunner.bulk.AbstractTaskExecutor;
import com.htec.examples.parallelrunner.bulk.BulkTaskRunner;
import com.htec.examples.parallelrunner.client.data.AugmentBulkData;
import com.htec.examples.parallelrunner.client.task.AddHashColumnTask;
import com.htec.util.JdbcAnyRecord;
import com.htec.util.Progress;
import org.eclipse.jdt.annotation.NonNullByDefault;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Bulk executor for augmentation tasks on batch of work.
 * Prepares the bulk infrastructure once and reuses it for all batches. Executes all per-record actions and reports failures.
 */
@NonNullByDefault
public class AugmentBulkDataExecutor extends AbstractTaskExecutor<AugmentBulkData> {

	private static final Duration DELAY_FAILURES_RETRIES = Duration.ofSeconds(5);

	/**
	 * Model for failure on the level of individual record.
	 */
	@NonNullByDefault
	public static class FailedAugmentationRecord {

		final JdbcAnyRecord m_record;
		final String m_error;
		final Exception m_exception;

		public FailedAugmentationRecord(JdbcAnyRecord record, String error, Exception exception) {
			m_record = record;
			m_error = error;
			m_exception = exception;
		}

		public JdbcAnyRecord getRecord() {
			return m_record;
		}

		public Exception getException() {
			return m_exception;
		}
	}

	private final Progress m_p;


	private final List<FailedAugmentationRecord> m_failures = new ArrayList<>();

	private int m_numberOfBatchedAugmentedRecords;

	private int m_numberOfAugmentedRecords;

	private boolean m_initialized = false;

	private int m_currentTaskRetryIndex = -1;

	public AugmentBulkDataExecutor(Progress p, BulkTaskRunner<AugmentBulkData> runner) {
		super(runner);
		m_p = p;
	}

	@Override
	protected void initialize() throws Exception {
		if(m_initialized) {
			throw new IllegalStateException("Already initialized!?");
		}

		initializeResources();

		m_initialized = true;
	}

	@Override
	protected void terminate() throws Exception {
		releaseResources();
	}

	@Override
	protected void executeOnce(AugmentBulkData data) {
		clearPerBatchState();
		m_currentTaskRetryIndex = data.getRetryIndex();

		checkForDelayAtStartup(data);

		for(JdbcAnyRecord record: data.getRecords()) {
			executeAugmentationPerRecord(record);
		}

		executeBatchDataUpdates();
	}

	private void clearPerBatchState() {
		m_currentTaskRetryIndex = -1;
		m_numberOfBatchedAugmentedRecords = 0;
		m_numberOfAugmentedRecords = 0;
		m_failures.clear();
	}

	private void checkForDelayAtStartup(AugmentBulkData data) {
		Duration startupDelay = data.getRetryIndex() == 0 ? null : DELAY_FAILURES_RETRIES;
		if(null != startupDelay) {
			try {
				Thread.sleep(startupDelay.toMillis());
			}catch(Exception ex) {
				ex.printStackTrace();
			}
		}
	}

	private void executeAugmentationPerRecord(JdbcAnyRecord record) {
		AddHashColumnTask task = new AddHashColumnTask(record);
		task.setOnCompleted(() -> {
			if(!m_p.isCancelled()) {
				m_p.increment(1);
			}
		});
		try {
			task.run(m_p);
			m_numberOfBatchedAugmentedRecords++;
		}catch(Exception ex) {
			m_failures.add(new FailedAugmentationRecord(record, ex.getLocalizedMessage(), ex));
		}
	}

	private void executeBatchDataUpdates() {
		if(m_numberOfBatchedAugmentedRecords > 0) {
			System.out.println("Executed batch changes of " + m_numberOfBatchedAugmentedRecords + " records!");
			m_numberOfAugmentedRecords += m_numberOfBatchedAugmentedRecords;
		}
	}
	public int getNumberOfAugmentedRecords() {
		return m_numberOfAugmentedRecords;
	}

	public List<FailedAugmentationRecord> getFailures() {
		return m_failures;
	}

	public int getCurrentTaskRetryIndex() {
		return m_currentTaskRetryIndex;
	}

	private void initializeResources() {

	}

	private void releaseResources() {

	}
}

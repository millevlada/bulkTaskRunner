package com.htec.examples.parallelrunner.client.data;

import com.htec.util.JdbcAnyRecord;
import org.eclipse.jdt.annotation.NonNullByDefault;

import java.util.List;

/**
 * Data class for one single batch of processing for augmentation.
 */
@NonNullByDefault
public class AugmentBulkData {

	/**
	 * Records to be processed inside a batch of work.
	 */
	private final List<JdbcAnyRecord> m_records;

	/**
	 * Retry index.
	 */
	private final int m_retryIndex;

	public AugmentBulkData(List<JdbcAnyRecord> records, int retryIndex) {
		m_records = records;
		m_retryIndex = retryIndex;
	}

	public List<JdbcAnyRecord> getRecords() {
		return m_records;
	}

	public int getRetryIndex() {
		return m_retryIndex;
	}
}

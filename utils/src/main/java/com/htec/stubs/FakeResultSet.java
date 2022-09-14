package com.htec.stubs;

import com.htec.util.JdbcAnyRecord;
import org.eclipse.jdt.annotation.NonNullByDefault;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Class represents stub result set.
 * Utility for showcases that do not require real database access.
 * Exercises common behavior as result set, like hasNext, next.
 * Initialized by required total number of stub data.
 */
@NonNullByDefault
public class FakeResultSet {

    private static final String[] FRUITS = {"Apple","Mango","Peach","Banana","Orange","Grapes","Watermelon","Tomato"};

    private static final String[] NAMES = {"Fred", "Jane", "Richard Nixon", "Miss America", "John Doe"};

    private static final String[] TRANSITIVE_VERB = {"loves", "hates", "sees", "knows", "looks for", "finds"};

    private int m_current = 0;

    private final int m_totalCount;

    public FakeResultSet(int totalCount) {
        m_totalCount = totalCount;
    }

    public boolean hasNext() {
        return m_totalCount > m_current;
    }

    public JdbcAnyRecord next() {
        if(!hasNext()) {
            throw new IllegalStateException("no more records in fake result set!");
        }
        JdbcAnyRecord rec = new JdbcAnyRecord();
        rec.set("ID", m_current);
        rec.set("col1", random(NAMES));
        rec.set("col2", random(TRANSITIVE_VERB));
        rec.set("col3", random(FRUITS));
        m_current++;
        return rec;
    }

    private String random(String[] names) {
        return names[ThreadLocalRandom.current().nextInt(names.length)];
    }

}

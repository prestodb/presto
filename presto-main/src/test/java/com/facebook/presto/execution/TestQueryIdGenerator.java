package com.facebook.presto.execution;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestQueryIdGenerator
{
    @Test
    public void testCreateNextQueryId()
            throws Exception
    {
        TestIdGenerator idGenerator = new TestIdGenerator();

        long millis = new DateTime(2001, 7, 14, 1, 2, 3, 4, DateTimeZone.UTC).getMillis();
        idGenerator.setNow(millis);

        // generate ids to 99,999
        for (int i = 0; i < 100_000; i++) {
            assertEquals(idGenerator.createNextQueryId(), new QueryId(String.format("20010714_010203_%05d_%s", i, idGenerator.getCoordinatorId())));
        }

        // next id will cause counter to roll, but we need to add a second to the time or code will block for ever
        millis += 1000;
        idGenerator.setNow(millis);
        for (int i = 0; i < 100_000; i++) {
            assertEquals(idGenerator.createNextQueryId(), new QueryId(String.format("20010714_010204_%05d_%s", i, idGenerator.getCoordinatorId())));
        }

        // more forward one more second and generate 100 ids
        millis += 1000;
        idGenerator.setNow(millis);
        for (int i = 0; i < 100; i++) {
            assertEquals(idGenerator.createNextQueryId(), new QueryId(String.format("20010714_010205_%05d_%s", i, idGenerator.getCoordinatorId())));
        }

        // now we move to the start of the next day, and the counter should reset
        millis = new DateTime(2001, 7, 15, 0, 0, 0, 0, DateTimeZone.UTC).getMillis();
        idGenerator.setNow(millis);
        for (int i = 0; i < 100_000; i++) {
            assertEquals(idGenerator.createNextQueryId(), new QueryId(String.format("20010715_000000_%05d_%s", i, idGenerator.getCoordinatorId())));
        }

    }

    private static class TestIdGenerator
            extends QueryIdGenerator
    {
        private long now;

        public String getCoordinatorId()
        {
            return coordinatorId;
        }

        public void setNow(long now)
        {
            this.now = now;
        }

        @Override
        protected long nowInMillis()
        {
            return now;
        }
    }
}

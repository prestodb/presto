/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.raptorx.storage.organization;

import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Set;

import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestChunkOrganizer
{
    @Test(timeOut = 5_000)
    public void testChunkOrganizerInProgress()
            throws Exception
    {
        ChunkOrganizer organizer = createChunkOrganizer();

        Set<Long> chunks = ImmutableSet.of(123L);
        OrganizationSet organizationSet = new OrganizationSet(1L, chunks, 10);

        organizer.enqueue(organizationSet);

        assertTrue(organizer.inProgress(getOnlyElement(chunks)));
        assertEquals(organizer.getChunksInProgress(), 1);

        while (organizer.inProgress(getOnlyElement(chunks))) {
            MILLISECONDS.sleep(10);
        }
        assertFalse(organizer.inProgress(getOnlyElement(chunks)));
        assertEquals(organizer.getChunksInProgress(), 0);
        organizer.shutdown();
    }

    public static class MockJobFactory
            implements JobFactory
    {
        @Override
        public Runnable create(OrganizationSet organizationSet)
        {
            return () -> sleepUninterruptibly(10, MILLISECONDS);
        }
    }

    public static ChunkOrganizer createChunkOrganizer()
    {
        return new ChunkOrganizer(new MockJobFactory(), 1);
    }
}

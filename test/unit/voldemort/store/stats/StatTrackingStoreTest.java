/*
 * $
 * $
 *
 * Copyright (C) 1999-2009 Jive Software. All rights reserved.
 *
 * This software is the proprietary information of Jive Software. Use is subject to license terms.
 */
package voldemort.store.stats;

import voldemort.store.AbstractByteArrayStoreTest;
import voldemort.store.Store;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;

import java.util.List;


public class StatTrackingStoreTest extends AbstractByteArrayStoreTest {

    private static final byte[] testVal = "test".getBytes();

    private List<ByteArray> keys = getKeys(10);
    @Override
    @SuppressWarnings("unchecked")
    public Store<ByteArray, byte[]> getStore() {
        return new StatTrackingStore(new InMemoryStorageEngine<ByteArray, byte[]>("test"), null);
    }


    public void testStats() throws Exception {
        StatTrackingStore<ByteArray, byte[]> store = (StatTrackingStore) getStore();

        // do a couple of puts
        for (int i = 1; i < 11; i++) {
            Thread.sleep(20);
            store.put(keys.get(i-1), new Versioned<byte[]>(testVal));

            // sleep again to make throughput # easier to validate
            if (i == 10) {
                Thread.sleep(20);
            }
        }

        assertEquals(10, store.getStats().getCount(Tracked.PUT));
        assertTrue(store.getStats().getAvgTimeInMs(Tracked.PUT) > 0);
        assertTrue(store.getStats().getThroughput(Tracked.PUT) > 0);
        // since we're pausing for 20 ms between requests we can't go over 50 ops a second
        // 51 because sleep doesn't seem to be exact on all os's
        assertTrue(store.getStats().getThroughput(Tracked.PUT) <= 51);
        
        assertEquals(0, store.getStats().getCount(Tracked.GET));
        assertEquals(0, store.getStats().getCount(Tracked.DELETE));
        assertEquals(0, store.getStats().getCount(Tracked.GET_ALL));
        assertEquals(0, store.getStats().getCount(Tracked.EXCEPTION));


        // do a couple of gets
        for (int i = 1; i < 11; i++) {
            Thread.sleep(20);
            store.get(keys.get(i-1));

            // sleep again to make throughput # easier to validate
            if (i == 10) {
                Thread.sleep(20);
            }
        }

        assertEquals(10, store.getStats().getCount(Tracked.GET));
        assertTrue(store.getStats().getAvgTimeInMs(Tracked.GET) > 0);
        assertTrue(store.getStats().getThroughput(Tracked.GET) > 0);
        // since we're pausing for 20 ms between requests we can't go over 50 ops a second
        // 51 because sleep doesn't seem to be exact on all os's
        assertTrue(store.getStats().getThroughput(Tracked.GET) <= 51);

        assertEquals(0, store.getStats().getCount(Tracked.DELETE));
        assertEquals(0, store.getStats().getCount(Tracked.GET_ALL));
        assertEquals(0, store.getStats().getCount(Tracked.EXCEPTION));

        // do a couple of getAlls
        for (int i = 0; i < 10; i++) {
            Thread.sleep(20);
            store.getAll(keys);

            // sleep again to make throughput # easier to validate
            if (i == 9) {
                Thread.sleep(20);
            }
        }

        assertEquals(10, store.getStats().getCount(Tracked.GET_ALL));
        assertTrue(store.getStats().getAvgTimeInMs(Tracked.GET_ALL) > 0);
        assertTrue(store.getStats().getThroughput(Tracked.GET_ALL) > 0);
        // since we're pausing for 20 ms between requests we can't go over 50 ops a second
        // 51 because sleep doesn't seem to be exact on all os's
        assertTrue(store.getStats().getThroughput(Tracked.GET_ALL) <= 51);

        assertEquals(0, store.getStats().getCount(Tracked.DELETE));
        assertEquals(0, store.getStats().getCount(Tracked.EXCEPTION));

        // do a couple of deletes
        for (int i = 1; i < 11; i++) {
            if (i > 1) {
                Thread.sleep(20);
            }
            ByteArray key = keys.get(i - 1);
            store.delete(key, store.get(key).get(0).getVersion());

            // sleep again to make throughput # easier to validate
            if (i == 10) {
                Thread.sleep(20);
            }
        }

        assertEquals(10, store.getStats().getCount(Tracked.DELETE));
        assertTrue(store.getStats().getAvgTimeInMs(Tracked.DELETE) > 0);
        assertTrue(store.getStats().getThroughput(Tracked.DELETE) > 0);
        // since we're pausing for 20 ms between requests we can't go over 50 ops a second
        // 51 because sleep doesn't seem to be exact on all os's
        assertTrue(store.getStats().getThroughput(Tracked.DELETE) <= 51);

        assertEquals(0, store.getStats().getCount(Tracked.EXCEPTION));
    }
}

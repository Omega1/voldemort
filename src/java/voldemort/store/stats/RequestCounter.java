package voldemort.store.stats;

import voldemort.utils.Time;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A thread-safe request counter that calculates throughput for a specified
 * duration of time on a sliding window basis.
 */
public class RequestCounter {

    private final Accumulator accumulator;

    /**
     * @param windowSizeMS specifies the duration of the sliding window (in milliseconds).
     * @param maxOpsToTrack the maximum operations to track
     */
    public RequestCounter(int windowSizeMS, int maxOpsToTrack) {
        this.accumulator = new Accumulator(windowSizeMS, maxOpsToTrack);
    }

    public long getCount() {
        return accumulator.getCount();
    }

    public long getTotalCount() {
        return accumulator.getTotal();
    }

    public float getThroughput() {
        double elapsed = (System.currentTimeMillis() - accumulator.getStartTimeMS())
                         / (double) Time.MS_PER_SECOND;
        if(elapsed > 0f) {
            return (float) (accumulator.getCount() / elapsed);
        } else {
            return -1f;
        }
    }

    public String getDisplayThroughput() {
        return String.format("%.2f", getThroughput());
    }

    public double getAverageTimeInMs() {
        return accumulator.getAverageTimeNS() / Time.NS_PER_MS;
    }

    public String getDisplayAverageTimeInMs() {
        return String.format("%.4f", getAverageTimeInMs());
    }

    /*
     * Updates the stats accumulator with another operation.
     *
     * @param timeNS time of operation, in nanoseconds
     */
    public void addOperation(long timeNS) {
        accumulator.addOperation(timeNS);
    }

    private static class Accumulator {

        private final AtomicLong total = new AtomicLong(0);


        private final long windowSizeMS;
        private final int maxOpsToTrack;

        // the index into the arrays
        private final AtomicInteger index = new AtomicInteger(0);

        private final long[] opSave;
        private final long[] opTime;

        public Accumulator(long windowSizeMS, int maxOpsToTrack) {
            this.windowSizeMS = windowSizeMS;
            this.maxOpsToTrack = maxOpsToTrack;

            opSave = new long[maxOpsToTrack];
            opTime = new long[maxOpsToTrack];

            Arrays.fill(opSave, -1);
            Arrays.fill(opTime, -1);
        }

        public void addOperation(long timeNS) {
            int idx = index.incrementAndGet() % maxOpsToTrack;
            opSave[idx] = System.nanoTime();
            opTime[idx] = timeNS;
            total.incrementAndGet();
        }

        public double getAverageTimeNS() {
            long count = getCount();
            return count > 0 ? 1f * getTotalTimeNS() / count : -0f;
        }

        public long getStartTimeMS() {
            // Under heavy load the arrays may not have anything in them
            // near the max time. If that's the case return the oldest
            // time in the array
            long start = System.currentTimeMillis();
            long max = start - windowSizeMS;
            long current = 0;
            long now = System.nanoTime();

            for (long saveTimeInNS : opSave) {
                if (saveTimeInNS == -1)
                    continue;

                long saveTimeInMS = (now - saveTimeInNS) / Time.NS_PER_MS;
                if (saveTimeInMS > current)
                    current = saveTimeInMS;

                if (current > max) {
                    return max;
                }
            }

            return current == 0 ? max : start - current;
        }

        public long getTotalTimeNS() {
            long totalTimeNS = 0;
            long now = System.nanoTime();

            for (int i = 0; i < opSave.length; i++) {
                long saveTimeInNS = opSave[i];
                // check to see if the op is in the window
                if (saveTimeInNS != -1 && now - saveTimeInNS <= windowSizeMS * Time.NS_PER_MS) {
                    long opTimeInNS = opTime[i];
                    if (opTimeInNS != -1) {
                        totalTimeNS += opTimeInNS;
                    }
                }
            }

            return totalTimeNS;
        }

        public long getCount() {
            long count = 0;
            long now = System.nanoTime();

            for (long saveTimeInNS : opSave) {
                // check to see if the op is in the window
                if (saveTimeInNS != -1 && now - saveTimeInNS <= windowSizeMS * Time.NS_PER_MS) {
                    count++;
                }
            }

            return count;
        }

        public long getTotal() {
            return total.get();
        }
    }

}
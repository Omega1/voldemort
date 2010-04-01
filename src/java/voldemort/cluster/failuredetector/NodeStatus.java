/*
 * Copyright 2008-2009 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.cluster.failuredetector;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Holds the status of a node--either available or unavailable as well as the
 * last date the status was checked.
 * 
 * Operations on this class are not atomic, but that is okay.
 * 
 * 
 */
class NodeStatus {

    private final AtomicLong lastChecked = new AtomicLong();

    private final AtomicBoolean isAvailable = new AtomicBoolean();

    private final AtomicLong startMillis = new AtomicLong();

    private final AtomicLong success = new AtomicLong();

    private final AtomicLong total = new AtomicLong();

    public long getLastChecked() {
        return lastChecked.get();
    }

    public void setLastChecked(long lastChecked) {
        this.lastChecked.set(lastChecked);
    }

    public boolean isAvailable() {
        return isAvailable.get();
    }

    public void setAvailable(boolean isAvailable) {
        this.isAvailable.set(isAvailable);
    }

    public long getStartMillis() {
        return startMillis.get();
    }

    public void setStartMillis(long startMillis) {
        this.startMillis.set(startMillis);
    }

    public long getSuccess() {
        return success.get();
    }

    public void setSuccess(long success) {
        this.success.set(success);
    }

    public void incrementSuccess(long delta) {
        this.success.addAndGet(delta);
    }

    public long getTotal() {
        return total.get();
    }

    public void setTotal(long total) {
        this.total.set(total);
    }

    public void incrementTotal(long delta) {
        this.total.addAndGet(delta);
    }

}

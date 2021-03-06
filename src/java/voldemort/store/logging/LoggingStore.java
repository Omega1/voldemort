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

package voldemort.store.logging;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.client.DeleteAllType;
import voldemort.store.DelegatingStore;
import voldemort.store.Store;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreUtils;
import voldemort.utils.SystemTime;
import voldemort.utils.Time;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * A store wrapper that handles debug logging.
 * 
 * 
 */
public class LoggingStore<K, V> extends DelegatingStore<K, V> {

    private final Logger logger;
    private final Time time;
    private final String instanceName;

    /**
     * Create a logging store that wraps the given store
     * 
     * @param store The store to wrap
     */
    public LoggingStore(Store<K, V> store) {
        this(store, new SystemTime());
    }

    /**
     * Create a logging store that wraps the given store
     * 
     * @param store The store to wrap
     * @param time The time implementation to use for computing ellapsed time
     */
    public LoggingStore(Store<K, V> store, Time time) {
        this(store, null, time);
    }

    /**
     * Create a logging store that wraps the given store
     * 
     * @param store The store to wrap
     * @param instance The instance name to display in logging messages
     * @param time The time implementation to use for computing ellapsed time
     */
    public LoggingStore(Store<K, V> store, String instance, Time time) {
        super(store);
        this.logger = Logger.getLogger(store.getClass());
        this.time = time;
        this.instanceName = instance == null ? ": " : instance + ": ";
    }

    @Override
    public void close() throws VoldemortException {
        if(logger.isDebugEnabled())
            logger.debug("Closing " + getName() + ".");
        super.close();
    }

    @Override
    public boolean delete(K key, Version version) throws VoldemortException {
        long startTimeNs = 0;
        boolean succeeded = false;
        if(logger.isDebugEnabled())
            startTimeNs = time.getNanoseconds();
        try {
            boolean deletedSomething = getInnerStore().delete(key, version);
            succeeded = true;
            return deletedSomething;
        } finally {
            printTimedMessage("DELETE", succeeded, startTimeNs);
        }
    }

    @Override
    public boolean deleteAll(Map<K, Version> keys) throws VoldemortException {
        StoreUtils.assertValidKeys(keys == null ? null : keys.keySet());
        long startTimeNs = 0;
        boolean deletedSomething = false;
        boolean succeeded = false;
        if(logger.isDebugEnabled())
            startTimeNs = time.getNanoseconds();

        try {
            for (Map.Entry<K, Version> entry : keys.entrySet()) {
                deletedSomething |= getInnerStore().delete(entry.getKey(), entry.getValue());
                succeeded = true;
            }
        } finally {
            printTimedMessage("DELETEALL", succeeded, startTimeNs);
        }
        
        return deletedSomething;
    }

    @Override
    public boolean deleteAll(DeleteAllType type, String expression) throws VoldemortException {
        long startTimeNs = 0;
        boolean deletedSomething = false;
        boolean succeeded = false;
        if(logger.isDebugEnabled())
            startTimeNs = time.getNanoseconds();

        try {
            deletedSomething = getInnerStore().deleteAll(type, expression);
            succeeded = true;
        } finally {
            printTimedMessage("DELETEALL", succeeded, startTimeNs);
        }

        return deletedSomething;
    }

    @Override
    public List<Versioned<V>> get(K key) throws VoldemortException {
        long startTimeNs = 0;
        boolean succeeded = false;
        if(logger.isDebugEnabled())
            startTimeNs = time.getNanoseconds();
        try {
            List<Versioned<V>> l = getInnerStore().get(key);
            succeeded = true;
            return l;
        } finally {
            printTimedMessage("GET", succeeded, startTimeNs);
        }
    }

    @Override
    public void put(K key, Versioned<V> value) throws VoldemortException {
        long startTimeNs = 0;
        boolean succeeded = false;
        if(logger.isDebugEnabled()) {
            startTimeNs = time.getNanoseconds();
        }
        try {
            getInnerStore().put(key, value);
            succeeded = true;
        } finally {
            printTimedMessage("PUT", succeeded, startTimeNs);
        }
    }

    private void printTimedMessage(String operation, boolean success, long startNs) {
        if(logger.isDebugEnabled()) {
            double elapsedMs = (time.getNanoseconds() - startNs) / (double) Time.NS_PER_MS;
            logger.debug(instanceName + operation + " operation on store '" + getName()
                         + "' completed " + (success ? "successfully" : "unsuccessfully") + " in "
                         + elapsedMs + " ms.");
        }
    }

    @Override
    public Object getCapability(StoreCapabilityType capability) {
        if(capability == StoreCapabilityType.LOGGER)
            return this.logger;
        else
            return getInnerStore().getCapability(capability);
    }

}

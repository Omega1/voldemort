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

package voldemort.store.configuration;

import static voldemort.TestUtils.getClock;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.commons.io.FileDeleteStrategy;

import voldemort.TestUtils;
import voldemort.store.AbstractStoreTest;
import voldemort.store.Store;
import voldemort.versioning.VectorClock;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public class ConfigurationStorageEngineTest extends AbstractStoreTest<String, String> {

    private File tempDir;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        if(null != tempDir && tempDir.exists())
            FileDeleteStrategy.FORCE.delete(tempDir);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        if(null != tempDir && tempDir.exists())
            FileDeleteStrategy.FORCE.delete(tempDir);
    }

    @Override
    public List<String> getKeys(int numKeys) {
        return getStrings(numKeys, 10);
    }

    @Override
    public Store<String, String> getStore() {
        if(null == tempDir || !tempDir.exists()) {
            tempDir = TestUtils.createTempDir();
        }
        return new ConfigurationStorageEngine("test", tempDir.getAbsolutePath());
    }

    @Override
    public List<String> getValues(int numValues) {
        return getStrings(numValues, 8);
    }

    @Override
    public void testDelete() {
        String key = getKey();
        Store<String, String> store = getStore();
        VectorClock c1 = getClock(1, 1);
        String value = getValue();

        // can't delete something that isn't there
        assertTrue(!store.delete(key, c1));

        store.put(key, new Versioned<String>(value, c1));
        assertEquals(1, store.get(key).size());

        // now delete that version too
        assertTrue("Delete failed!", store.delete(key, c1));
        assertEquals(0, store.get(key).size());
    }

    @Override
    public void testDeleteAll() {
        String key = getKey();
        Store<String, String> store = getStore();
        VectorClock c1 = getClock(1, 1);
        String value = getValue();

        // can't delete something that isn't there
        Map<String, Version> keys = Maps.newHashMap();
        keys.put(key, c1);
        assertTrue(!store.deleteAll(keys));

        store.put(key, new Versioned<String>(value, c1));
        assertEquals(1, store.get(key).size());

        // now delete that version too
        assertTrue("Delete failed!", store.deleteAll(keys));
        assertEquals(0, store.get(key).size());
    }

    @Override
    public void testGetAndDeleteNonExistentKey() {
        try {
            assertEquals("Size should be 0", 0, getStore().get("unknown_key").size());
        } catch(Exception e) {
            fail();
        }
    }

    @Override
    public void testNullKeys() {
        // insert of null keys should not be allowed
        try {
            getStore().put("test.key", new Versioned<String>(null));
            fail();
        } catch(Exception e) {
            // expected
        }
    }

    @Override
    protected boolean allowConcurrentOperations() {
        return false;
    }

    public void testEmacsTempFile() throws IOException {
        Store<String, String> store = getStore();
        String keyName = "testkey.xml";

        store.put(keyName, new Versioned<String>("testValue"));
        assertEquals("Only one file of name key should be present.", 1, store.get(keyName).size());

        // Now create a emacs style temp file
        new File(tempDir, keyName + "#").createNewFile();
        new File(tempDir, "#" + keyName + "#").createNewFile();
        new File(tempDir, keyName + "~").createNewFile();
        new File(tempDir, "." + keyName + "~").createNewFile();

        assertEquals("Only one file of name key should be present.", 1, store.get(keyName).size());

        // do a new put
        VectorClock clock = (VectorClock) store.get(keyName).get(0).getVersion();
        store.put(keyName, new Versioned<String>("testValue1", clock.incremented(0, 1)));
        assertEquals("Only one file of name key should be present.", 1, store.get(keyName).size());
        assertEquals("Value should match.", "testValue1", store.get(keyName).get(0).getValue());

        // try getAll
        Map<String, List<Versioned<String>>> map = store.getAll(Arrays.asList(keyName));
        assertEquals("Only one file of name key should be present.", 1, map.get(keyName).size());
        assertEquals("Value should match.", "testValue1", map.get(keyName).get(0).getValue());
    }
}

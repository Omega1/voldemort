package voldemort.store;

import voldemort.VoldemortException;
import voldemort.client.DeleteAllType;
import voldemort.store.memory.InMemoryStorageEngine;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import java.util.List;
import java.util.Map;

public class FailingReadsStore<K, V> implements Store<K, V> {

    private final String name;
    private final InMemoryStorageEngine<K, V> engine;

    public FailingReadsStore(String name) {
        this.name = name;
        this.engine = new InMemoryStorageEngine<K, V>(name);
    }

    public void close() throws VoldemortException {}

    public boolean delete(K key, Version version) throws VoldemortException {
        return engine.delete(key, version);
    }

    public boolean deleteAll(Map<K, Version> keys) throws VoldemortException {
        return engine.deleteAll(keys);
    }

    public boolean deleteAll(DeleteAllType type, String expression) throws VoldemortException {
        return engine.deleteAll(type, expression);
    }

    public List<Versioned<V>> get(K key) throws VoldemortException {
        throw new VoldemortException("Operation failed");
    }

    public java.util.List<Version> getVersions(K key) {
        throw new VoldemortException("Operation failed");
    }

    public Map<K, List<Versioned<V>>> getAll(Iterable<K> keys) throws VoldemortException {
        throw new VoldemortException("Operation failed");
    }

    public Object getCapability(StoreCapabilityType capability) {
        throw new NoSuchCapabilityException(capability, getName());
    }

    public String getName() {
        return name;
    }

    public void put(K key, Versioned<V> value) throws VoldemortException {
        engine.put(key, value);
    }
}

/*
 * Copyright 2009 Mustard Grain, Inc.
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

package voldemort.server;

import voldemort.annotations.jmx.JmxGetter;
import voldemort.annotations.jmx.JmxManaged;
import voldemort.utils.JmxUtils;

/**
 * AbstractSocketService abstracts the different implementations so that we can
 * use this common super class by various callers.
 * 
 */

@JmxManaged(description = "A server that handles remote operations on stores via TCP/IP.")
public abstract class AbstractSocketService extends AbstractService implements VoldemortService {

    protected final String host;

    protected final int port;

    protected final String serviceName;

    protected final boolean enableJmx;

    public AbstractSocketService(ServiceType type, String host, int port, String serviceName, boolean enableJmx) {
        super(type);
        this.host = host;
        this.port = port;
        this.serviceName = serviceName;
        this.enableJmx = enableJmx;
    }

    /**
     * Simply retrieves the host on which this service is listening for incoming
     * requests.
     *
     * @return the host string
     */

    @JmxGetter(name = "host", description = "The host on which the server is accepting connections.")
    public String getHost() {
        return host;
    }

    /**
     * Simply retrieves the port on which this service is listening for incoming
     * requests.
     * 
     * @return Port number
     */

    @JmxGetter(name = "port", description = "The port on which the server is accepting connections.")
    public final int getPort() {
        return port;
    }

    /**
     * Returns a StatusManager instance for use with status reporting tools.
     * 
     * @return StatusManager
     */

    public abstract StatusManager getStatusManager();

    /**
     * If JMX is enabled, will register the given object under the service name
     * with which this class was created.
     * 
     * @param obj Object to register as an MBean
     */

    protected void enableJmx(Object obj) {
        if(enableJmx)
            JmxUtils.registerMbean(serviceName, obj);
    }

}

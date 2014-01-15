/**
 * Copyright 2013 Benjamin Lerer
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.horizondb.db;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;

/**
 * Base class for <code>Component</code> instance. This class manage the state of the component.
 * 
 * @author Benjamin
 * 
 */
public abstract class AbstractComponent implements Component {

    /**
     * The logger.
     */
    protected final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * The status of this component.
     */
    private final AtomicReference<Status> status = new AtomicReference<>(Status.NOT_STARTED);

    /**
     * {@inheritDoc}
     */
    @Override
    public final String getName() {
        return MetricRegistry.name(this.getClass());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void start() throws InterruptedException, IOException {

        if (!this.status.compareAndSet(Status.NOT_STARTED, Status.STARTING)) {

            throw new IllegalStateException("component " + getName()
                    + " has already been started or is in the process of starting");
        }

        try {

            this.logger.info("component {} is starting up", getName());

            doStart();

            this.logger.info("component {} has succesfully started", getName());

        } catch (Exception e) {

            this.status.set(Status.NOT_STARTED);
            throw e;
        }
        this.status.set(Status.RUNNING);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void shutdown() throws InterruptedException {

        if (!this.status.compareAndSet(Status.RUNNING, Status.SHUTTING_DOWN)) {

            throw new IllegalStateException("component " + getName()
                    + "is not in a running state so it cannot be shutdown");
        }

        try {

            this.logger.info("{} is shutting down", getName());

            doShutdown();

        } finally {

            this.status.set(Status.SHUTDOWN);
        }

        this.logger.info("component {} has succesfully shutdown", getName());
    }

    /**
     * Returns <code>true</code> if this component is running, <code>false</code> otherwise.
     * 
     * @return <code>true</code> if this component is running, <code>false</code> otherwise.
     */
    public final boolean isRunning() {

        return this.status.get() == Status.RUNNING;
    }

    /**
     * Checks if the component is running and throw an <code>IllegalStateException</code> if it is not.
     * 
     * @throws IllegalStateException if the component is not in a running state.
     */
    protected final void checkRunning() {

        if (!isRunning()) {
            throw new IllegalStateException("The " + getName() + " component is not in a running state.");
        }
    }

    /**
     * Performs the actual component startup.
     * 
     * @throws IOException if an IO problem occurs during startup.
     * @throws InterruptedException if the thread has been interrupted.
     */
    protected abstract void doStart() throws IOException, InterruptedException;

    /**
     * Performs the actual shutdown.
     * 
     * @throws InterruptedException if the thread has been interrupted.
     */
    protected abstract void doShutdown() throws InterruptedException;

    /**
     * The possible status for this component.
     */
    protected static enum Status {

        NOT_STARTED, STARTING, RUNNING, SHUTTING_DOWN, SHUTDOWN;
    }
}

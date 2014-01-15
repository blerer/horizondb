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

import io.horizondb.db.metrics.Monitorable;

import java.io.IOException;

/**
 * Describes the lifecycle of a database component.
 * 
 * @author Benjamin
 * 
 */
public interface Component extends Monitorable {

    /**
     * Start this database component.
     * 
     * @throws IOException if an IO problem occurs during startup.
     * @throws InterruptedException if the thread has been interrupted.
     */
    void start() throws IOException, InterruptedException;

    /**
     * Shutdown this database component.
     * 
     * @throws InterruptedException if the thread has been interrupted.
     */
    void shutdown() throws InterruptedException;
}

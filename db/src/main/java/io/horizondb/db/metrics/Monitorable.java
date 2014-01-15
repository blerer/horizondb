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
package io.horizondb.db.metrics;

import com.codahale.metrics.MetricRegistry;

/**
 * Allow a component to register and unregister metrics.
 * 
 * @author Benjamin
 * 
 */
public interface Monitorable extends Nameable {

    /**
     * Register the metrics of this <code>Monitorable</code> object.
     * 
     * @param registry the registry to which the metrics must be registered.
     */
    void register(MetricRegistry registry);

    /**
     * Unregister the metrics of this <code>Monitorable</code> object.
     * 
     * @param registry the registry from which the metrics must be unregistered.
     */
    void unregister(MetricRegistry registry);
}

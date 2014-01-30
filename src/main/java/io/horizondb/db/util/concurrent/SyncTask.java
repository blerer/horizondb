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
package io.horizondb.db.util.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

/**
 * <code>Callable</code> that can be used to make sure that all the task previously submitted to a single threaded
 * {@link ExecutorService} have been executed.
 * 
 * @author Benjamin
 * 
 */
public final class SyncTask implements Callable<Boolean> {

    /**
     * {@inheritDoc}
     */
    @Override
    public Boolean call() {
        return Boolean.TRUE;
    }
}

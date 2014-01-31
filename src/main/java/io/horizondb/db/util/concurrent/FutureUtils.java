/**
 * Copyright 2014 Benjamin Lerer
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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Utility methods to work with <code>Futures</code>.
 * 
 * @author Benjamin
 *
 */
public final class FutureUtils {

    /**
     * Utility method to get the result of a future when it should not be possible to get an exception
     * with a get call.
     * 
     * @param future the future for which the value must be returned
     * @return the future result
     */
    public static <V> V safeGet(Future<V> future) {
        
        try {
            
            return future.get();
            
        } catch (InterruptedException e) {

            Thread.currentThread().interrupt();
            throw new IllegalStateException("An unexpected exception has occured", e);
            
        } catch (ExecutionException e) {
            throw new IllegalStateException("An unexpected exception has occured", e);
        }
    }

    /**
     * The class must not be instantiated.
     */
    private FutureUtils() {
    }
}

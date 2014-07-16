/**
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
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import static org.junit.Assert.assertFalse;

/**
 * @author Benjamin
 *
 */
public class CountDownFutureTest {

    @Test
    public void testCountDown() throws Exception {
        
        final CountDownFuture<Boolean> future = new CountDownFuture<Boolean>(Boolean.TRUE, 2);
        
        Callable<Boolean> callable = new Callable<Boolean>() {
            
            @Override
            public Boolean call() throws Exception {

                    return future.get();
            }
        };
        
        ExecutorService executor = Executors.newSingleThreadExecutor();
        
        try {
            Future<Boolean> completionMonitor = executor.submit(callable);
            
            assertFalse(completionMonitor.isDone());
            future.countDown();
            Thread.sleep(100);
            assertFalse(completionMonitor.isDone());
            future.countDown();
            future.get(100, TimeUnit.MILLISECONDS);
        } finally {
            executor.shutdown();
        }
        
    }

}

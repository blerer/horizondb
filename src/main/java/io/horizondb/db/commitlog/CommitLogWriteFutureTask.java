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
package io.horizondb.db.commitlog;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.util.concurrent.ExecutionList;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * <code>FutureTask</code> that is used to ensure that the future will be in the completed state only 
 * once the data have been written and flushed to the disk.
 * 
 * @author Benjamin
 *
 */
final class CommitLogWriteFutureTask<V> extends FutureTask<V> implements ListenableFuture<V> {

    /**
     * The execution list used to hold the listeners.
     */
    private final ExecutionList executionList = new ExecutionList();
    
    /**
     * The latch that will make thread await until the written data has been flushed to the disk.
     */
    private CountDownLatch flushSignal = new CountDownLatch(1);

    
    /**
     * Creates a <code>FutureTask</code> that is used to ensure that the future will be in the completed 
     * state only once the data have been written and flushed to the disk.
     * 
     * @param writeFuture the write <code>Future</code>.
     */
    public CommitLogWriteFutureTask(Callable<V> callable) {
        super(callable);
    }

    /**
     * Signal that Sets the flush future associated to this write.    
     */
    void flushed() {
        this.flushSignal.countDown();
        notifyListeners();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V get() throws InterruptedException, ExecutionException {
        
        V result = super.get();
        waitForFlush();

        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        
        long timeInNanos = System.nanoTime();
        
        V result = super.get(timeout, unit);
        
        long usedTime = System.nanoTime() - timeInNanos;
        long remainingTime = unit.toNanos(timeout) - usedTime;
        
        waitForFlush(remainingTime, TimeUnit.NANOSECONDS);
        
        return result;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void addListener(Runnable runnable, Executor executor) {
        this.executionList.add(runnable, executor);
    }

    /**
     * Notify the listeners.
     */
    private void notifyListeners() {

        this.executionList.execute();
    }
    
    /**
     * Wait for the data to be flushed on the disk.
     * 
     * @throws InterruptedException if the thread is interrupted
     * @throws ExecutionException if a problem occurs while flushing the data
     */
    private void waitForFlush() throws InterruptedException, ExecutionException {
        this.flushSignal.await();
    }

    /**
     * Waits if necessary for at most the given time for the data to be flushed on the disk.
     *
     * @param timeout the maximum time to wait
     * @param unit the time unit of the timeout argument
     * @return the computed result
     * @throws InterruptedException if the thread is interrupted
     * @throws ExecutionException if a problem occurs while flushing the data.
     * @throws TimeoutException if the wait timed out
     */
    private void waitForFlush(long timeout, TimeUnit timeUnit) throws InterruptedException, ExecutionException, TimeoutException {
        this.flushSignal.await(timeout, timeUnit);
    }
}

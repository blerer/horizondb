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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * <code>RunnableScheduledFuture</code> that is used to ensure that the future will be in the completed state only 
 * once the data have been written and flushed to the disk.
 * 
 * @author Benjamin
 *
 */
final class CommitLogWriteFuture<V> implements RunnableScheduledFuture<V>, ListenableFuture<V> {

    /**
     * The write task future.
     */
    private final RunnableScheduledFuture<V> writeFuture;
    
    /**
     * The latch that will make thread await until the written data has been flushed to the disk.
     */
    private CountDownLatch flushSignal = new CountDownLatch(1);

    /**
     * Creates a <code>RunnableScheduledFuture</code> that is used to ensure that the future will be in the completed 
     * state only once the data have been written and flushed to the disk.
     * 
     * @param writeFuture the write <code>Future</code>.
     */
    public CommitLogWriteFuture(RunnableScheduledFuture<V> writeFuture) {
        this.writeFuture = writeFuture;
    }

    /**
     * Signal that Sets the flush future associated to this write.    
     */
    void flushed() {
        this.flushSignal.countDown();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void run() {
        this.writeFuture.run();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return this.writeFuture.cancel(mayInterruptIfRunning);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isCancelled() {
        return this.writeFuture.isCancelled();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isDone() {
        return this.writeFuture.isDone() && (this.flushSignal.getCount() == 0);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V get() throws InterruptedException, ExecutionException {
        
        V replayPosition = this.writeFuture.get();
        waitForFlush();

        return replayPosition;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        
        long timeInNanos = System.nanoTime();
        
        V replayPosition = this.writeFuture.get(timeout, unit);
        
        long usedTime = System.nanoTime() - timeInNanos;
        long remainingTime = unit.toNanos(timeout) - usedTime;
        
        waitForFlush(remainingTime, TimeUnit.NANOSECONDS);
        
        return replayPosition;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getDelay(TimeUnit unit) {
        
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int compareTo(Delayed o) {
        
        return -o.compareTo(this.writeFuture);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isPeriodic() {
        return false;
    }
    
    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public void addListener(Runnable runnable, Executor executor) {
        ((ListenableFuture<V>) this.writeFuture).addListener(runnable, executor);
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

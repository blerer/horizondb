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

import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A <code>RunnableScheduledFuture</code> which forwards all its method calls to another 
 * <code>RunnableScheduledFuture</code>. Subclasses should override one or more methods to modify the behavior of the 
 * backing <code>RunnableScheduledFuture</code> as desired per the decorator pattern. 
 * 
 * @author Benjamin
 *
 */
public abstract class ForwardingRunnableScheduledFuture<V> implements RunnableScheduledFuture<V> {

    /**
     * {@inheritDoc}
     */
    @Override
    public void run() {
        delegate().run();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return delegate().cancel(mayInterruptIfRunning);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isCancelled() {
        return delegate().isCancelled();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isDone() {
        return delegate().isDone();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V get() throws InterruptedException, ExecutionException {
        return delegate().get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return delegate().get(timeout, unit);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getDelay(TimeUnit unit) {
        return delegate().getDelay(unit);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int compareTo(Delayed o) {
        return delegate().compareTo(o);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isPeriodic() {
        return delegate().isPeriodic();
    }

    protected abstract RunnableScheduledFuture<V> delegate();
}

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
package io.horizondb.db.series.filters;

import io.horizondb.db.series.Filter;

import java.io.IOException;

/**
 * <code>Filter</code> that accept everything.
 * 
 * @author Benjamin
 *
 */
public final class NoopFilter<T> implements Filter<T> {

    /**
     * The class instance.
     */
    @SuppressWarnings("rawtypes")
    private static final NoopFilter INSTANCE = new NoopFilter();
    
    /**
     * Returns the <code>NoopFilter</code> instance.
     * @return the <code>NoopFilter</code> instance.
     */
    public static <T> Filter<T> instance() {
        
        return INSTANCE;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean accept(T value) throws IOException {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isDone() {
        return false;
    }
    
    /**
     * The class is a singleton.
     */
    private NoopFilter() {
        
    }
}

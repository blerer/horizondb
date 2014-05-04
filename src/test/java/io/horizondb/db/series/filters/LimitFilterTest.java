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

import org.junit.Test;

import com.google.common.collect.Range;

import static org.junit.Assert.*;

/**
 * @author Benjamin
 *
 */
public class LimitFilterTest {

    @SuppressWarnings("boxing")
    @Test
    public void testAcceptWithLimitLargerThanTheNumberOfAcceptedValues() throws IOException {

        Filter<Integer> filter = new LimitFilter<>(new RangeFilter<Integer>(Range.closed(2, 3)), 10);
        assertFalse(filter.isDone());
        assertFalse(filter.accept(1));
        assertFalse(filter.isDone());
        assertTrue(filter.accept(2));        
        assertFalse(filter.isDone());
        assertTrue(filter.accept(3));
        assertFalse(filter.isDone());
        assertTrue(filter.accept(3));
        assertFalse(filter.isDone());
        assertFalse(filter.accept(4));
        assertFalse(filter.isDone());
        assertTrue(filter.accept(3));
        assertFalse(filter.isDone());
    }

    @SuppressWarnings("boxing")
    @Test
    public void testAcceptWithLimitLowerThanTheNumberOfAcceptedValues() throws IOException {

        Filter<Integer> filter = new LimitFilter<>(new RangeFilter<Integer>(Range.closed(2, 3)), 2);
        assertFalse(filter.isDone());
        assertFalse(filter.accept(1));
        assertFalse(filter.isDone());
        assertTrue(filter.accept(2));        
        assertFalse(filter.isDone());
        assertTrue(filter.accept(3));
        assertFalse(filter.isDone());
        assertFalse(filter.accept(3));
        assertTrue(filter.isDone());
    }
    
    @SuppressWarnings("boxing")
    @Test
    public void testAcceptWithOffset() throws IOException {

        Filter<Integer> filter = new LimitFilter<>(new RangeFilter<Integer>(Range.closed(1, 3)), 1, 2);
        assertFalse(filter.isDone());
        assertFalse(filter.accept(1));
        assertFalse(filter.isDone());
        assertTrue(filter.accept(2));        
        assertFalse(filter.isDone());
        assertTrue(filter.accept(3));
        assertFalse(filter.isDone());
        assertFalse(filter.accept(3));
        assertTrue(filter.isDone());
    }
    
    @SuppressWarnings("boxing")
    @Test
    public void testAcceptWithOffsetAndValuesNeverDecreasing() throws IOException {

        Filter<Integer> filter = new LimitFilter<>(new RangeFilter<Integer>(Range.closed(1, 3), true), 2, 10);
        assertFalse(filter.isDone());
        assertFalse(filter.accept(1));
        assertFalse(filter.isDone());
        assertFalse(filter.accept(2));        
        assertFalse(filter.isDone());
        assertTrue(filter.accept(3));
        assertFalse(filter.isDone());
        assertTrue(filter.accept(3));
        assertFalse(filter.isDone());
        assertFalse(filter.accept(4));
        assertTrue(filter.isDone());
    }

    @SuppressWarnings("boxing")
    @Test
    public void testAcceptWithOffsetAndValuesNeverDecreasingAndLimitReached() throws IOException {

        Filter<Integer> filter = new LimitFilter<>(new RangeFilter<Integer>(Range.closed(1, 3), true), 2, 1);
        assertFalse(filter.isDone());
        assertFalse(filter.accept(1));
        assertFalse(filter.isDone());
        assertFalse(filter.accept(2));        
        assertFalse(filter.isDone());
        assertTrue(filter.accept(3));
        assertFalse(filter.isDone());
        assertFalse(filter.accept(3));
        assertTrue(filter.isDone());
    }
}

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
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Benjamin
 *
 */
public class InFilterTest {

    @SuppressWarnings("boxing")
    @Test
    public void testAccept() throws IOException {

        SortedSet<Integer> set = new TreeSet<>();
        set.add(2);
        set.add(4);
        
        Filter<Integer> filter = new InFilter<Integer>(set);
        assertFalse(filter.isDone());
        assertFalse(filter.accept(1));
        assertFalse(filter.isDone());
        assertTrue(filter.accept(2));        
        assertFalse(filter.isDone());
        assertFalse(filter.accept(3));
        assertFalse(filter.isDone());
        assertTrue(filter.accept(4));
        assertFalse(filter.isDone());
        assertTrue(filter.accept(4));
        assertFalse(filter.isDone());
        assertFalse(filter.accept(5));
        assertFalse(filter.isDone());
    }

    @SuppressWarnings("boxing")
    @Test
    public void testAcceptWithValueNeverDecreasing() throws IOException {

        SortedSet<Integer> set = new TreeSet<>();
        set.add(2);
        set.add(4);
        
        Filter<Integer> filter = new InFilter<Integer>(set, true);
        assertFalse(filter.isDone());
        assertFalse(filter.accept(1));
        assertFalse(filter.isDone());
        assertTrue(filter.accept(2));        
        assertFalse(filter.isDone());
        assertFalse(filter.accept(3));
        assertFalse(filter.isDone());
        assertTrue(filter.accept(4));
        assertFalse(filter.isDone());
        assertTrue(filter.accept(4));
        assertFalse(filter.isDone());
        assertFalse(filter.accept(5));
        assertTrue(filter.isDone());
    }
}

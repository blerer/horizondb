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
package io.horizondb.db.series;

import io.horizondb.db.series.MapKeyValueIterator;
import io.horizondb.db.series.MergingKeyValueIterator;

import java.io.IOException;
import java.util.Map;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Benjamin
 *
 */
public class MergingKeyValueIteratorTest {

    @Test
    public void testWithNoIterators() throws IOException {
        
        MergingKeyValueIterator<String, String> merging = new MergingKeyValueIterator<>();
        assertFalse(merging.next());
    }
    
    @Test
    public void testWithOneEmptyIterators() throws IOException {
        
        Map<String, String> map = ImmutableMap.of();
                
        MapKeyValueIterator<String, String> iterator = new MapKeyValueIterator<>(map);
        
        MergingKeyValueIterator<String, String> merging = new MergingKeyValueIterator<>(iterator);

        assertFalse(merging.next());
    }

    @Test
    public void testWithOneIterators() throws IOException {
        
        Map<String, String> map = ImmutableMap.of("A", "1", "B", "2");
                
        MapKeyValueIterator<String, String> iterator = new MapKeyValueIterator<>(map);
        
        MergingKeyValueIterator<String, String> merging = new MergingKeyValueIterator<>(iterator);

        assertTrue(merging.next());
        assertEquals("A", merging.getKey());
        assertEquals("1", merging.getValue());
        assertTrue(merging.next());
        assertEquals("B", merging.getKey());
        assertEquals("2", merging.getValue());
        assertFalse(merging.next());
    }
    
    @Test
    public void testWithTwoIteratorsFollowingEachOther() throws IOException {
        
        Map<String, String> map = ImmutableMap.of("A", "1", "B", "2");
                
        MapKeyValueIterator<String, String> iterator = new MapKeyValueIterator<>(map);
        
        Map<String, String> map2 = ImmutableMap.of("C", "3", "D", "4");
        
        MapKeyValueIterator<String, String> iterator2 = new MapKeyValueIterator<>(map2);
        
        MergingKeyValueIterator<String, String> merging = new MergingKeyValueIterator<>(iterator, iterator2);

        assertTrue(merging.next());
        assertEquals("A", merging.getKey());
        assertEquals("1", merging.getValue());
        assertTrue(merging.next());
        assertEquals("B", merging.getKey());
        assertEquals("2", merging.getValue());
        assertTrue(merging.next());
        assertEquals("C", merging.getKey());
        assertEquals("3", merging.getValue());
        assertTrue(merging.next());
        assertEquals("D", merging.getKey());
        assertEquals("4", merging.getValue());
        assertFalse(merging.next());
    }
    
    @Test
    public void testWithTwoIterators() throws IOException {
        
        Map<String, String> map = ImmutableMap.of("A", "1", "C", "3");
                
        MapKeyValueIterator<String, String> iterator = new MapKeyValueIterator<>(map);
        
        Map<String, String> map2 = ImmutableMap.of("B", "2", "D", "4");
        
        MapKeyValueIterator<String, String> iterator2 = new MapKeyValueIterator<>(map2);
        
        MergingKeyValueIterator<String, String> merging = new MergingKeyValueIterator<>(iterator, iterator2);

        assertTrue(merging.next());
        assertEquals("A", merging.getKey());
        assertEquals("1", merging.getValue());
        assertTrue(merging.next());
        assertEquals("B", merging.getKey());
        assertEquals("2", merging.getValue());
        assertTrue(merging.next());
        assertEquals("C", merging.getKey());
        assertEquals("3", merging.getValue());
        assertTrue(merging.next());
        assertEquals("D", merging.getKey());
        assertEquals("4", merging.getValue());
        assertFalse(merging.next());
    }
}

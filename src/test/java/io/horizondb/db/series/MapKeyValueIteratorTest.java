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

import java.util.Collections;
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
public class MapKeyValueIteratorTest {

    @Test
    public void testEmptyMap() {
        
        MapKeyValueIterator<String, String> iterator = new MapKeyValueIterator<>(Collections.<String, String>emptyMap());
        assertFalse(iterator.next());
    }

    @Test
    public void testWithOneMapEntry() {
        
        Map<String, String> map = ImmutableMap.of("A", "1");
        
        MapKeyValueIterator<String, String> iterator = new MapKeyValueIterator<>(map);
        assertTrue(iterator.next());
        assertEquals("A", iterator.getKey());
        assertEquals("1", iterator.getValue());
        assertFalse(iterator.next());
    }
    
    @Test
    public void testWithTwoMapEntry() {
        
        Map<String, String> map = ImmutableMap.of("A", "1", "B", "2");
        
        MapKeyValueIterator<String, String> iterator = new MapKeyValueIterator<>(map);
        assertTrue(iterator.next());
        assertEquals("A", iterator.getKey());
        assertEquals("1", iterator.getValue());
        assertTrue(iterator.next());
        assertEquals("B", iterator.getKey());
        assertEquals("2", iterator.getValue());
        assertFalse(iterator.next());
    }
    
    @Test
    public void testWithTwoMapEntriesInWrongOrder() {
        
        Map<String, String> map = ImmutableMap.of("B", "2", "A", "1");
        
        MapKeyValueIterator<String, String> iterator = new MapKeyValueIterator<>(map);
        assertTrue(iterator.next());
        assertEquals("A", iterator.getKey());
        assertEquals("1", iterator.getValue());
        assertTrue(iterator.next());
        assertEquals("B", iterator.getKey());
        assertEquals("2", iterator.getValue());
        assertFalse(iterator.next());
    }
    
    @Test
    public void testWithMultipleMapEntriesInWrongOrder() {
        
        Map<String, String> map = ImmutableMap.of("B", "2", "C", "3", "A", "1", "D", "4");
        
        MapKeyValueIterator<String, String> iterator = new MapKeyValueIterator<>(map);
        assertTrue(iterator.next());
        assertEquals("A", iterator.getKey());
        assertEquals("1", iterator.getValue());
        assertTrue(iterator.next());
        assertEquals("B", iterator.getKey());
        assertEquals("2", iterator.getValue());
        assertTrue(iterator.next());
        assertEquals("C", iterator.getKey());
        assertEquals("3", iterator.getValue());
        assertTrue(iterator.next());
        assertEquals("D", iterator.getKey());
        assertEquals("4", iterator.getValue());
        assertFalse(iterator.next());
    }
}

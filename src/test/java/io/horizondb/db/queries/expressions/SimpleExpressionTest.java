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
package io.horizondb.db.queries.expressions;

import io.horizondb.db.queries.Expression;
import io.horizondb.model.core.Field;
import io.horizondb.model.schema.FieldType;

import org.junit.Test;

import com.google.common.collect.RangeSet;

import static io.horizondb.model.core.util.TimeUtils.EUROPE_BERLIN_TIMEZONE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Benjamin
 *
 */
public class SimpleExpressionTest {

    @Test
    public void testGetTimestampRangesWithNonTimestampField() {
        
        Expression expr = Expressions.gt("price", "10"); 
        Field prototype = FieldType.MILLISECONDS_TIMESTAMP.newField();
        
        RangeSet<Field> rangeSet = expr.getTimestampRanges(prototype, EUROPE_BERLIN_TIMEZONE);
        assertEquals(prototype.allValues(), rangeSet);
    }
    
    @Test
    public void testGetTimestampRangesWithGT() {
        
        long timeInMillis = 1399147894150L;
        
        Expression expr = Expressions.gt("timestamp", timeInMillis + "ms"); 
        Field prototype = FieldType.MILLISECONDS_TIMESTAMP.newField();
        
        RangeSet<Field> rangeSet = expr.getTimestampRanges(prototype, EUROPE_BERLIN_TIMEZONE);
        
        Field expected = FieldType.MILLISECONDS_TIMESTAMP.newField();
        expected.setTimestampInMillis(timeInMillis + 10);
        
        assertTrue(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(timeInMillis - 10);
        
        assertFalse(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(timeInMillis);
        
        assertFalse(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(Long.MAX_VALUE);
        
        assertTrue(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(Long.MIN_VALUE);
        
        assertFalse(rangeSet.contains(expected));
    }

    @Test
    public void testGetTimestampRangesWithGE() {
        
        long timeInMillis = 1399147894150L;
        
        Expression expr = Expressions.ge("timestamp", timeInMillis + "ms"); 
        Field prototype = FieldType.MILLISECONDS_TIMESTAMP.newField();
        
        RangeSet<Field> rangeSet = expr.getTimestampRanges(prototype, EUROPE_BERLIN_TIMEZONE);
        
        Field expected = FieldType.MILLISECONDS_TIMESTAMP.newField();
        expected.setTimestampInMillis(timeInMillis + 10);
        
        assertTrue(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(timeInMillis - 10);
        
        assertFalse(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(timeInMillis);
        
        assertTrue(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(Long.MAX_VALUE);
        
        assertTrue(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(Long.MIN_VALUE);
        
        assertFalse(rangeSet.contains(expected));
    }
    
    @Test
    public void testGetTimestampRangesWithLE() {
        
        long timeInMillis = 1399147894150L;
        
        Expression expr = Expressions.le("timestamp", timeInMillis + "ms"); 
        Field prototype = FieldType.MILLISECONDS_TIMESTAMP.newField();
        
        RangeSet<Field> rangeSet = expr.getTimestampRanges(prototype, EUROPE_BERLIN_TIMEZONE);
        
        Field expected = FieldType.MILLISECONDS_TIMESTAMP.newField();
        expected.setTimestampInMillis(timeInMillis + 10);
        
        assertFalse(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(timeInMillis - 10);
        
        assertTrue(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(timeInMillis);
        
        assertTrue(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(Long.MAX_VALUE);
        
        assertFalse(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(0);
        
        assertTrue(rangeSet.contains(expected));
    }
    
    @Test
    public void testGetTimestampRangesWithLT() {
        
        long timeInMillis = 1399147894150L;
        
        Expression expr = Expressions.lt("timestamp", timeInMillis + "ms"); 
        Field prototype = FieldType.MILLISECONDS_TIMESTAMP.newField();
        
        RangeSet<Field> rangeSet = expr.getTimestampRanges(prototype, EUROPE_BERLIN_TIMEZONE);
        
        Field expected = FieldType.MILLISECONDS_TIMESTAMP.newField();
        expected.setTimestampInMillis(timeInMillis + 10);
        
        assertFalse(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(timeInMillis - 10);
        
        assertTrue(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(timeInMillis);
        
        assertFalse(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(Long.MAX_VALUE);
        
        assertFalse(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(0);
        
        assertTrue(rangeSet.contains(expected));
    }
    
    @Test
    public void testGetTimestampRangesWithEQ() {
        
        long timeInMillis = 1399147894150L;
        
        Expression expr = Expressions.eq("timestamp", timeInMillis + "ms"); 
        Field prototype = FieldType.MILLISECONDS_TIMESTAMP.newField();
        
        RangeSet<Field> rangeSet = expr.getTimestampRanges(prototype, EUROPE_BERLIN_TIMEZONE);
        
        Field expected = FieldType.MILLISECONDS_TIMESTAMP.newField();
        expected.setTimestampInMillis(timeInMillis + 10);
        
        assertFalse(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(timeInMillis - 10);
        
        assertFalse(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(timeInMillis);
        
        assertTrue(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(Long.MAX_VALUE);
        
        assertFalse(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(0);
        
        assertFalse(rangeSet.contains(expected));
    }
    
    @Test
    public void testGetTimestampRangesWithNE() {
        
        long timeInMillis = 1399147894150L;
        
        Expression expr = Expressions.ne("timestamp", timeInMillis + "ms"); 
        Field prototype = FieldType.MILLISECONDS_TIMESTAMP.newField();
        
        RangeSet<Field> rangeSet = expr.getTimestampRanges(prototype, EUROPE_BERLIN_TIMEZONE);
        
        Field expected = FieldType.MILLISECONDS_TIMESTAMP.newField();
        expected.setTimestampInMillis(timeInMillis + 10);
        
        assertTrue(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(timeInMillis - 10);
        
        assertTrue(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(timeInMillis);
        
        assertFalse(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(Long.MAX_VALUE);
        
        assertTrue(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(0);
        
        assertTrue(rangeSet.contains(expected));
    }
}

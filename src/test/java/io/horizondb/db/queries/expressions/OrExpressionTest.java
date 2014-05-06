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

import java.util.Arrays;

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
public class OrExpressionTest {

    @Test
    public void testGetTimestampRangesWithNonTimestampField() {
        
        Expression left = Expressions.gt("price", "10"); 
        Expression right = Expressions.lt("price", "12"); 
        Field prototype = FieldType.MILLISECONDS_TIMESTAMP.newField();
        
        Expression expr = Expressions.or(left, right);
        
        RangeSet<Field> rangeSet = expr.getTimestampRanges(prototype, EUROPE_BERLIN_TIMEZONE);
        assertEquals(prototype.allValues(), rangeSet);
    }
    
    @Test
    public void testGetTimestampRangesWithGTAndNonTimestampField() {
        
        long timeInMillis = 1399147894150L;
        
        Expression left = Expressions.gt("timestamp", timeInMillis + "ms"); 
        Expression right = Expressions.lt("price", "12"); 
        
        Expression expr = Expressions.or(left, right); 
        Field prototype = FieldType.MILLISECONDS_TIMESTAMP.newField();
        
        RangeSet<Field> rangeSet = expr.getTimestampRanges(prototype, EUROPE_BERLIN_TIMEZONE);
        
        assertEquals(prototype.allValues(), rangeSet);
    }
    
    @Test
    public void testGetTimestampRangesWithGTAndLT() {
        
        long timeInMillis = 1399147894150L;
        
        Expression left = Expressions.gt("timestamp", (timeInMillis + 100) + "ms"); 
        Expression rigth = Expressions.lt("timestamp", timeInMillis + "ms"); 
        
        Expression expr = Expressions.or(left, rigth); 
        Field prototype = FieldType.MILLISECONDS_TIMESTAMP.newField();
        
        RangeSet<Field> rangeSet = expr.getTimestampRanges(prototype, EUROPE_BERLIN_TIMEZONE);
        
        Field expected = FieldType.MILLISECONDS_TIMESTAMP.newField();
        expected.setTimestampInMillis(timeInMillis + 10);
        
        assertFalse(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(timeInMillis - 10);
        
        assertTrue(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(timeInMillis + 200);
        
        assertTrue(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(timeInMillis);
        
        assertFalse(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(Long.MAX_VALUE);
        
        assertTrue(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(Long.MIN_VALUE);
        
        assertTrue(rangeSet.contains(expected));
    }
    
    @Test
    public void testGetTimestampRangesWithOpposedGTAndLT() {
        
        long timeInMillis = 1399147894150L;
        
        Expression left = Expressions.lt("timestamp", (timeInMillis + 100) + "ms"); 
        Expression rigth = Expressions.gt("timestamp", timeInMillis + "ms"); 
        
        Expression expr = Expressions.or(left, rigth); 
        Field prototype = FieldType.MILLISECONDS_TIMESTAMP.newField();
        
        RangeSet<Field> rangeSet = expr.getTimestampRanges(prototype, EUROPE_BERLIN_TIMEZONE);
        
        assertEquals(prototype.allValues(), rangeSet);
    }
        
    @Test
    public void testGetTimestampRangesWithInAndBetween() {
        
        long timeInMillis = 1399147894150L;
        
        Expression left = Expressions.in("timestamp", Arrays.asList((timeInMillis - 10) + "ms",
                                                                     timeInMillis + "ms",
                                                                     (timeInMillis + 10) + "ms")); 
        
        Expression rigth = Expressions.between("timestamp", (timeInMillis - 20) + "ms", (timeInMillis + 20) + "ms");
        
        Expression expr = Expressions.or(left, rigth);
        
        Field prototype = FieldType.MILLISECONDS_TIMESTAMP.newField();
        
        RangeSet<Field> rangeSet = expr.getTimestampRanges(prototype, EUROPE_BERLIN_TIMEZONE);
        
        Field expected = FieldType.MILLISECONDS_TIMESTAMP.newField();
        
        expected.setTimestampInMillis(timeInMillis + 10);
        
        assertTrue(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(timeInMillis + 20);
        
        assertTrue(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(timeInMillis + 25);
        
        assertFalse(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(timeInMillis + 15);
        
        assertTrue(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(timeInMillis - 10);
        
        assertTrue(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(timeInMillis - 15);
        
        assertTrue(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(timeInMillis - 20);
        
        assertTrue(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(timeInMillis - 25);
        
        assertFalse(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(timeInMillis);
        
        assertTrue(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(Long.MAX_VALUE);
        
        assertFalse(rangeSet.contains(expected));
        
        expected.setTimestampInMillis(Long.MIN_VALUE);
        
        assertFalse(rangeSet.contains(expected));
    }
}

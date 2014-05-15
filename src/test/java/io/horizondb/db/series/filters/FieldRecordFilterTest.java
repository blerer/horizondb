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
import io.horizondb.model.core.Field;
import io.horizondb.model.core.fields.ImmutableField;
import io.horizondb.model.core.records.TimeSeriesRecord;
import io.horizondb.model.schema.RecordTypeDefinition;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;

import org.junit.Test;

import com.google.common.collect.Range;

import static io.horizondb.model.core.util.TimeUtils.EUROPE_BERLIN_TIMEZONE;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Benjamin
 *
 */
public class FieldRecordFilterTest {

    @Test
    public void testAcceptWithOnlyOneRecordType() throws IOException {
        
        RecordTypeDefinition trade = RecordTypeDefinition.newBuilder("Trade")
                                                         .addDecimalField("price")
                                                         .addIntegerField("volume")
                                                         .build();
        
        TimeSeriesDefinition tsDefinition = TimeSeriesDefinition.newBuilder("DAX")
                                                                .addRecordType(trade)
                                                                .build();
        
        Field field = tsDefinition.newField("volume");
        field.setValueFromString(EUROPE_BERLIN_TIMEZONE, "12");
        
        Filter<Field> filter = new EqualityFilter<Field>(ImmutableField.of(field));
        
        FieldRecordFilter recordFilter = new FieldRecordFilter(tsDefinition, "volume", filter);
    
        TimeSeriesRecord record = tsDefinition.newRecord("Trade");
        record.setTimestampInMillis(0, 1000);
        record.setDecimal(1, 12, 0);
        record.setInt(2, 11);
        
        assertFalse(recordFilter.accept(record));
        
        record.setDelta(true);
        record.setTimestampInMillis(0, 1000);
        record.setDecimal(1, 0, 0);
        record.setInt(2, 1);
        
        assertTrue(recordFilter.accept(record));
        
        record.setDelta(true);
        record.setTimestampInMillis(0, 500);
        record.setDecimal(1, 1, 0);
        record.setInt(2, 1);
        
        assertFalse(recordFilter.accept(record));
        
        record.setDelta(false);
        record.setTimestampInMillis(0, 3000);
        record.setDecimal(1, 12, 0);
        record.setInt(2, 12);
        
        assertTrue(recordFilter.accept(record));
    }
    
    @Test
    public void testAcceptWithTwoRecordTypeAndFieldInBoth() throws IOException {
        
        RecordTypeDefinition quoteType = RecordTypeDefinition.newBuilder("Quote")
                                                         .addDecimalField("bidPrice")
                                                         .addDecimalField("askPrice")
                                                         .addIntegerField("bidVolume")
                                                         .addIntegerField("askVolume")
                                                         .addByteField("exchangeState")
                                                         .build();
        
        RecordTypeDefinition tradeType = RecordTypeDefinition.newBuilder("Trade")
                                                         .addDecimalField("price")
                                                         .addIntegerField("volume")
                                                         .addByteField("exchangeState")
                                                         .build();
        
        TimeSeriesDefinition tsDefinition = TimeSeriesDefinition.newBuilder("DAX")
                                                                .addRecordType(tradeType)
                                                                .addRecordType(quoteType)
                                                                .build();
        
        Field field = tsDefinition.newField("exchangeState");
        field.setValueFromString(EUROPE_BERLIN_TIMEZONE, "1");
        
        Filter<Field> filter = new EqualityFilter<Field>(ImmutableField.of(field));
        
        FieldRecordFilter recordFilter = new FieldRecordFilter(tsDefinition, "exchangeState", filter);
            
        TimeSeriesRecord quote = tsDefinition.newRecord("Quote");
        quote.setTimestampInMillis(0, 1000);
        quote.setDecimal(1, 12, 0);
        quote.setDecimal(2, 13, 0);
        quote.setInt(3, 11);
        quote.setInt(4, 5);
        quote.setByte(5, 2);
        
        assertFalse(recordFilter.accept(quote));
        
        quote.setDelta(true);
        quote.setTimestampInMillis(0, 1000);
        quote.setDecimal(1, 5, 1);
        quote.setDecimal(2, 0, 0);
        quote.setInt(3, -5);
        quote.setInt(4, 0);
        quote.setByte(5, 0);
        
        assertFalse(recordFilter.accept(quote));
        
        TimeSeriesRecord trade = tsDefinition.newRecord("Trade");
        trade.setTimestampInMillis(0, 2500);
        trade.setDecimal(1, 125, 1);
        trade.setInt(2, 3);
        trade.setByte(3, 1);
        
        assertTrue(recordFilter.accept(trade));
        
        trade.setDelta(true);
        trade.setTimestampInMillis(0, 1000);
        trade.setDecimal(1, 0, 0);
        trade.setInt(2, 3);
        trade.setByte(3, 0);
        
        assertTrue(recordFilter.accept(trade));
        
        quote.setDelta(true);
        quote.setTimestampInMillis(0, 1500);
        quote.setDecimal(1, -5, 1);
        quote.setDecimal(2, 0, 0);
        quote.setInt(3, 5);
        quote.setInt(4, 0);
        quote.setByte(5, -1);
        
        assertTrue(recordFilter.accept(quote));
        
        trade.setDelta(true);
        trade.setTimestampInMillis(0, 500);
        trade.setDecimal(1, -5, 1);
        trade.setInt(2, 4);
        trade.setByte(3, 0);
        
        assertTrue(recordFilter.accept(trade));
        
        quote.setDelta(false);
        quote.setTimestampInMillis(0, 4000);
        quote.setDecimal(1, 12, 0);
        quote.setDecimal(2, 13, 0);
        quote.setInt(3, 7);
        quote.setInt(4, 2);
        quote.setByte(5, 1);
        
        assertTrue(recordFilter.accept(quote));
        
        quote.setDelta(true);
        quote.setTimestampInMillis(0, 500);
        quote.setDecimal(1, 0, 0);
        quote.setDecimal(2, 0, 0);
        quote.setInt(3, 0);
        quote.setInt(4, 0);
        quote.setByte(5, 1);
        
        assertFalse(recordFilter.accept(quote));
    }
    
    @Test
    public void testAcceptWithTwoRecordTypeAndFieldOnlyInOne() throws IOException {
        
        RecordTypeDefinition quoteType = RecordTypeDefinition.newBuilder("Quote")
                                                         .addDecimalField("bidPrice")
                                                         .addDecimalField("askPrice")
                                                         .addIntegerField("bidVolume")
                                                         .addIntegerField("askVolume")
                                                         .addByteField("exchangeState")
                                                         .build();
        
        RecordTypeDefinition tradeType = RecordTypeDefinition.newBuilder("Trade")
                                                         .addDecimalField("price")
                                                         .addIntegerField("volume")
                                                         .addByteField("exchangeState")
                                                         .build();
        
        TimeSeriesDefinition tsDefinition = TimeSeriesDefinition.newBuilder("DAX")
                                                                .addRecordType(tradeType)
                                                                .addRecordType(quoteType)
                                                                .build();
        
        Field field = tsDefinition.newField("volume");
        field.setValueFromString(EUROPE_BERLIN_TIMEZONE, "10");
        
        Filter<Field> filter = new RangeFilter<Field>(Range.closed(field.minValue() , ImmutableField.of(field)));
        
        FieldRecordFilter recordFilter = new FieldRecordFilter(tsDefinition, "volume", filter);
            
        TimeSeriesRecord quote = tsDefinition.newRecord("Quote");
        quote.setTimestampInMillis(0, 1000);
        quote.setDecimal(1, 12, 0);
        quote.setDecimal(2, 13, 0);
        quote.setInt(3, 11);
        quote.setInt(4, 5);
        quote.setByte(5, 2);
        
        assertFalse(recordFilter.accept(quote));
        
        quote.setDelta(true);
        quote.setTimestampInMillis(0, 1000);
        quote.setDecimal(1, 5, 1);
        quote.setDecimal(2, 0, 0);
        quote.setInt(3, -5);
        quote.setInt(4, 0);
        quote.setByte(5, 0);
        
        assertFalse(recordFilter.accept(quote));
        
        TimeSeriesRecord trade = tsDefinition.newRecord("Trade");
        trade.setTimestampInMillis(0, 2500);
        trade.setDecimal(1, 125, 1);
        trade.setInt(2, 3);
        trade.setByte(3, 1);
        
        assertTrue(recordFilter.accept(trade));
        
        trade.setDelta(true);
        trade.setTimestampInMillis(0, 1000);
        trade.setDecimal(1, 0, 0);
        trade.setInt(2, 3);
        trade.setByte(3, 0);
        
        assertTrue(recordFilter.accept(trade));
        
        quote.setDelta(true);
        quote.setTimestampInMillis(0, 1500);
        quote.setDecimal(1, -5, 1);
        quote.setDecimal(2, 0, 0);
        quote.setInt(3, 5);
        quote.setInt(4, 0);
        quote.setByte(5, -1);
        
        assertFalse(recordFilter.accept(quote));
        
        trade.setDelta(true);
        trade.setTimestampInMillis(0, 500);
        trade.setDecimal(1, -5, 1);
        trade.setInt(2, 4);
        trade.setByte(3, 0);
        
        assertTrue(recordFilter.accept(trade));
        
        quote.setDelta(false);
        quote.setTimestampInMillis(0, 4000);
        quote.setDecimal(1, 12, 0);
        quote.setDecimal(2, 13, 0);
        quote.setInt(3, 7);
        quote.setInt(4, 2);
        quote.setByte(5, 1);
        
        assertFalse(recordFilter.accept(quote));
        
        quote.setDelta(true);
        quote.setTimestampInMillis(0, 500);
        quote.setDecimal(1, 0, 0);
        quote.setDecimal(2, 0, 0);
        quote.setInt(3, 0);
        quote.setInt(4, 0);
        quote.setByte(5, 1);
        
        assertFalse(recordFilter.accept(quote));
    }
    
    @Test
    public void testAcceptWithTwoRecordTypeAndFilteringOnTimestamp() throws IOException {
        
        RecordTypeDefinition quoteType = RecordTypeDefinition.newBuilder("Quote")
                                                         .addDecimalField("bidPrice")
                                                         .addDecimalField("askPrice")
                                                         .addIntegerField("bidVolume")
                                                         .addIntegerField("askVolume")
                                                         .addByteField("exchangeState")
                                                         .build();
        
        RecordTypeDefinition tradeType = RecordTypeDefinition.newBuilder("Trade")
                                                         .addDecimalField("price")
                                                         .addIntegerField("volume")
                                                         .addByteField("exchangeState")
                                                         .build();
        
        TimeSeriesDefinition tsDefinition = TimeSeriesDefinition.newBuilder("DAX")
                                                                .addRecordType(tradeType)
                                                                .addRecordType(quoteType)
                                                                .build();
        
        Field field = tsDefinition.newField("timestamp");
        field.setValueFromString(EUROPE_BERLIN_TIMEZONE, "3000ms");
        
        Filter<Field> filter = new RangeFilter<Field>(Range.closed(ImmutableField.of(field), field.maxValue()), true);
        
        FieldRecordFilter recordFilter = new FieldRecordFilter(tsDefinition, "timestamp", filter);
            
        TimeSeriesRecord quote = tsDefinition.newRecord("Quote");
        quote.setTimestampInMillis(0, 1000);
        quote.setDecimal(1, 12, 0);
        quote.setDecimal(2, 13, 0);
        quote.setInt(3, 11);
        quote.setInt(4, 5);
        quote.setByte(5, 2);
        
        assertFalse(recordFilter.accept(quote));
        assertFalse(recordFilter.isDone());
        
        quote.setDelta(true);
        quote.setTimestampInMillis(0, 1000);
        quote.setDecimal(1, 5, 1);
        quote.setDecimal(2, 0, 0);
        quote.setInt(3, -5);
        quote.setInt(4, 0);
        quote.setByte(5, 0);
        
        assertFalse(recordFilter.accept(quote));
        assertFalse(recordFilter.isDone());
        
        TimeSeriesRecord trade = tsDefinition.newRecord("Trade");
        trade.setTimestampInMillis(0, 2500);
        trade.setDecimal(1, 125, 1);
        trade.setInt(2, 3);
        trade.setByte(3, 1);
        
        assertFalse(recordFilter.accept(trade));
        assertFalse(recordFilter.isDone());
        
        trade.setDelta(true);
        trade.setTimestampInMillis(0, 1000);
        trade.setDecimal(1, 0, 0);
        trade.setInt(2, 3);
        trade.setByte(3, 0);
        
        assertTrue(recordFilter.accept(trade));
        assertFalse(recordFilter.isDone());
        
        quote.setDelta(true);
        quote.setTimestampInMillis(0, 1500);
        quote.setDecimal(1, -5, 1);
        quote.setDecimal(2, 0, 0);
        quote.setInt(3, 5);
        quote.setInt(4, 0);
        quote.setByte(5, -1);
        
        assertTrue(recordFilter.accept(quote));
        assertFalse(recordFilter.isDone());
        
        trade.setDelta(true);
        trade.setTimestampInMillis(0, 500);
        trade.setDecimal(1, -5, 1);
        trade.setInt(2, 4);
        trade.setByte(3, 0);
        
        assertTrue(recordFilter.accept(trade));
        assertFalse(recordFilter.isDone());
        
        quote.setDelta(false);
        quote.setTimestampInMillis(0, 4000);
        quote.setDecimal(1, 12, 0);
        quote.setDecimal(2, 13, 0);
        quote.setInt(3, 7);
        quote.setInt(4, 2);
        quote.setByte(5, 1);
        
        assertTrue(recordFilter.accept(quote));
        assertFalse(recordFilter.isDone());
        
        quote.setDelta(true);
        quote.setTimestampInMillis(0, 500);
        quote.setDecimal(1, 0, 0);
        quote.setDecimal(2, 0, 0);
        quote.setInt(3, 0);
        quote.setInt(4, 0);
        quote.setByte(5, 1);
        
        assertTrue(recordFilter.accept(quote));
        assertFalse(recordFilter.isDone());
    }
    
    @Test
    public void testIsDoneWithTwoRecordTypeAndFilteringOnTimestamp() throws IOException {
        
        RecordTypeDefinition quoteType = RecordTypeDefinition.newBuilder("Quote")
                                                         .addDecimalField("bidPrice")
                                                         .addDecimalField("askPrice")
                                                         .addIntegerField("bidVolume")
                                                         .addIntegerField("askVolume")
                                                         .addByteField("exchangeState")
                                                         .build();
        
        RecordTypeDefinition tradeType = RecordTypeDefinition.newBuilder("Trade")
                                                         .addDecimalField("price")
                                                         .addIntegerField("volume")
                                                         .addByteField("exchangeState")
                                                         .build();
        
        TimeSeriesDefinition tsDefinition = TimeSeriesDefinition.newBuilder("DAX")
                                                                .addRecordType(tradeType)
                                                                .addRecordType(quoteType)
                                                                .build();
        
        Field field = tsDefinition.newField("timestamp");
        field.setValueFromString(EUROPE_BERLIN_TIMEZONE, "3000ms");
        
        Filter<Field> filter = new RangeFilter<Field>(Range.closed(field.minValue(), ImmutableField.of(field)), true);
        
        FieldRecordFilter recordFilter = new FieldRecordFilter(tsDefinition, "timestamp", filter);
            
        TimeSeriesRecord quote = tsDefinition.newRecord("Quote");
        quote.setTimestampInMillis(0, 1000);
        quote.setDecimal(1, 12, 0);
        quote.setDecimal(2, 13, 0);
        quote.setInt(3, 11);
        quote.setInt(4, 5);
        quote.setByte(5, 2);
        
        assertTrue(recordFilter.accept(quote));
        assertFalse(recordFilter.isDone());
        
        quote.setDelta(true);
        quote.setTimestampInMillis(0, 1000);
        quote.setDecimal(1, 5, 1);
        quote.setDecimal(2, 0, 0);
        quote.setInt(3, -5);
        quote.setInt(4, 0);
        quote.setByte(5, 0);
        
        assertTrue(recordFilter.accept(quote));
        assertFalse(recordFilter.isDone());
        
        TimeSeriesRecord trade = tsDefinition.newRecord("Trade");
        trade.setTimestampInMillis(0, 2500);
        trade.setDecimal(1, 125, 1);
        trade.setInt(2, 3);
        trade.setByte(3, 1);
        
        assertTrue(recordFilter.accept(trade));
        assertFalse(recordFilter.isDone());
        
        trade.setDelta(true);
        trade.setTimestampInMillis(0, 1000);
        trade.setDecimal(1, 0, 0);
        trade.setInt(2, 3);
        trade.setByte(3, 0);
        
        assertFalse(recordFilter.accept(trade));
        assertTrue(recordFilter.isDone());
    }
}

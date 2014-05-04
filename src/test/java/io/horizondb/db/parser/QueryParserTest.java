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
package io.horizondb.db.parser;

import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import io.horizondb.db.HorizonDBException;
import io.horizondb.db.Query;
import io.horizondb.db.queries.CreateDatabaseQuery;
import io.horizondb.db.queries.CreateTimeSeriesQuery;
import io.horizondb.db.queries.SelectQuery;
import io.horizondb.db.queries.UseDatabaseQuery;
import io.horizondb.model.schema.DatabaseDefinition;
import io.horizondb.model.schema.RecordTypeDefinition;
import io.horizondb.model.schema.TimeSeriesDefinition;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author Benjamin
 *
 */
public class QueryParserTest {

    @Test
    public void testParseCreateDatabase() throws HorizonDBException {
       
        Query query = QueryParser.parse(" CREATE DATABASE TEST;");
        
        assertNotNull(query);
        assertTrue(query instanceof CreateDatabaseQuery);
        
        query = QueryParser.parse("CREATE DATABASE test ;");
        
        assertNotNull(query);
        assertEquals(new DatabaseDefinition("test"), ((CreateDatabaseQuery) query).getDefinition());
    }
    
    @Test
    public void testParseUseDatabase() throws HorizonDBException {
       
        Query query = QueryParser.parse("USE TEST;");
        
        assertNotNull(query);
        assertTrue(query instanceof UseDatabaseQuery);
    }
    
    @Test
    public void testParseCreateTimeSeries() throws HorizonDBException {

        RecordTypeDefinition quote = RecordTypeDefinition.newBuilder("Quote")
                                                         .addNanosecondTimestampField("received")
                                                         .addDecimalField("bidPrice")
                                                         .addDecimalField("askPrice")
                                                         .addIntegerField("bidVolume")
                                                         .addIntegerField("askVolume")
                                                         .build();

        RecordTypeDefinition trade = RecordTypeDefinition.newBuilder("Trade")
                                                         .addNanosecondTimestampField("received")
                                                         .addDecimalField("price")
                                                         .addIntegerField("volume")
                                                         .build();

        TimeSeriesDefinition expected = TimeSeriesDefinition.newBuilder("Dax")
                                                            .addRecordType(quote)
                                                            .addRecordType(trade)
                                                            .timeUnit(TimeUnit.NANOSECONDS)
                                                            .timeZone(TimeZone.getTimeZone("Europe/Berlin"))
                                                            .build();
        
        Query query = QueryParser.parse("CREATE TIMESERIES Dax (" +
                                        "Quote(received NANOSECONDS_TIMESTAMP, bidPrice DECIMAL, askPrice DECIMAL, bidVolume INTEGER, askVolume INTEGER), " +
                                        "Trade(received NANOSECONDS_TIMESTAMP, price DECIMAL, volume INTEGER))TIME_UNIT = NANOSECONDS TIMEZONE = 'Europe/Berlin';");
        assertNotNull(query);
        
        assertEquals(expected, ((CreateTimeSeriesQuery) query).getDefinition());
    }
    
    @Test
    public void testParseCreateTimeSeriesWithDefaultTimeUnit() throws HorizonDBException {

        RecordTypeDefinition quote = RecordTypeDefinition.newBuilder("Quote")
                                                         .addNanosecondTimestampField("received")
                                                         .addDecimalField("bidPrice")
                                                         .addDecimalField("askPrice")
                                                         .addIntegerField("bidVolume")
                                                         .addIntegerField("askVolume")
                                                         .build();

        RecordTypeDefinition trade = RecordTypeDefinition.newBuilder("Trade")
                                                         .addNanosecondTimestampField("received")
                                                         .addDecimalField("price")
                                                         .addIntegerField("volume")
                                                         .build();

        TimeSeriesDefinition expected = TimeSeriesDefinition.newBuilder("Dax")
                                                            .addRecordType(quote)
                                                            .addRecordType(trade)
                                                            .timeUnit(TimeUnit.MILLISECONDS)
                                                            .timeZone(TimeZone.getTimeZone("America/Los_Angeles"))
                                                            .build();
        
        Query query = QueryParser.parse("CREATE TIMESERIES Dax (" +
                                        "Quote(received NANOSECONDS_TIMESTAMP, bidPrice DECIMAL, askPrice DECIMAL, bidVolume INTEGER, askVolume INTEGER), " +
                                        "Trade(received NANOSECONDS_TIMESTAMP, price DECIMAL, volume INTEGER))TIMEZONE = 'America/Los_Angeles';");
        assertNotNull(query);
        
        assertEquals(expected, ((CreateTimeSeriesQuery) query).getDefinition());
    }
    
    @Test
    public void testParseSelectAll() throws HorizonDBException {

        
        SelectQuery query = (SelectQuery) QueryParser.parse("SELECT * FROM Dax;");
        assertEquals("Dax", query.getTimeSeriesName());
        assertEquals("", query.getExpression().toString());
    }
    
    @Test
    public void testParseSelectWithTimestampGreaterThan() throws HorizonDBException {

        String expression = "timestamp > 2";
        
        SelectQuery query = (SelectQuery) QueryParser.parse("SELECT * FROM Dax WHERE " + expression + ";");
        assertEquals("Dax", query.getTimeSeriesName());
        assertEquals(expression, query.getExpression().toString());
    }
    
    @Test
    public void testParseSelectWithAndAndOr() throws HorizonDBException {

        String expression = "timestamp > 2 AND timestamp < 4 OR timestamp > 6 AND timestamp < 8";
        
        SelectQuery query = (SelectQuery) QueryParser.parse("SELECT * FROM Dax WHERE " + expression + ";");
        
        assertEquals("Dax", query.getTimeSeriesName());
        assertEquals(expression, query.getExpression().toString());
    }
    
    @Test
    public void testParseSelectWithAndOrAndParentheses() throws HorizonDBException {

        String expression = "timestamp > 2 AND (timestamp < 4 OR timestamp > 6)";
        
        SelectQuery query = (SelectQuery) QueryParser.parse("SELECT * FROM Dax WHERE " + expression + ";");
        
        assertEquals("Dax", query.getTimeSeriesName());
        assertEquals(expression, query.getExpression().toString());
    }
    
    @Test
    public void testParseSelectWithInExpression() throws HorizonDBException {

        String expression = "timestamp IN (2, 4)";
        
        SelectQuery query = (SelectQuery) QueryParser.parse("SELECT * FROM Dax WHERE " + expression + ";");
        
        assertEquals("Dax", query.getTimeSeriesName());
        assertEquals(expression, query.getExpression().toString());
    }
    
    @Test
    public void testParseSelectWithBetweenExpression() throws HorizonDBException {

        String expression = "timestamp BETWEEN 2 AND 4";
        
        SelectQuery query = (SelectQuery) QueryParser.parse("SELECT * FROM Dax WHERE " + expression + ";");
        
        assertEquals("Dax", query.getTimeSeriesName());
        assertEquals(expression, query.getExpression().toString());
    }
    
    @Test
    public void testParseSelectWithNotBetweenExpression() throws HorizonDBException {

        String expression = "timestamp NOT BETWEEN 2 AND 6";
        
        SelectQuery query = (SelectQuery) QueryParser.parse("SELECT * FROM Dax WHERE " + expression + ";");
        
        assertEquals("Dax", query.getTimeSeriesName());
        assertEquals(expression, query.getExpression().toString());
    }
    
    @Test
    public void testParseSelectWithBetweenAndInExpression() throws HorizonDBException {

        String expression = "timestamp BETWEEN 2 AND 6 AND timestamp IN (5, 6)";
        
        SelectQuery query = (SelectQuery) QueryParser.parse("SELECT * FROM Dax WHERE " + expression + ";");
        
        assertEquals("Dax", query.getTimeSeriesName());
        assertEquals(expression, query.getExpression().toString());
    }
    
    @Test
    public void testParseSelectWithNotInExpression() throws HorizonDBException {

        String expression = "timestamp NOT IN (2, 4)";
        
        SelectQuery query = (SelectQuery) QueryParser.parse("SELECT * FROM Dax WHERE " + expression + ";");
        
        assertEquals("Dax", query.getTimeSeriesName());
        assertEquals(expression, query.getExpression().toString());
    }
    
    @Test
    public void testParseSelectWithInExpressionAndOnlyOneValue() throws HorizonDBException {

        String expression = "timestamp IN (2)";
        
        SelectQuery query = (SelectQuery) QueryParser.parse("SELECT * FROM Dax WHERE " + expression + ";");
        
        assertEquals("Dax", query.getTimeSeriesName());
        assertEquals(expression, query.getExpression().toString());
    }
    
    @Test
    public void testParseWithInvalidQuery() throws HorizonDBException {
       
        try {
            
            QueryParser.parse("CREATE DB TEST;");
            fail();
            
        } catch (BadHqlGrammarException e) {
            assertTrue(true); 
        }
    }
}

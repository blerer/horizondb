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

import io.horizondb.db.HorizonDBException;
import io.horizondb.model.protocol.CreateDatabasePayload;
import io.horizondb.model.protocol.CreateTimeSeriesPayload;
import io.horizondb.model.protocol.HqlQueryPayload;
import io.horizondb.model.protocol.Msg;
import io.horizondb.model.protocol.OpCode;
import io.horizondb.model.protocol.SelectPayload;
import io.horizondb.model.protocol.UseDatabasePayload;
import io.horizondb.model.schema.DatabaseDefinition;
import io.horizondb.model.schema.RecordTypeDefinition;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Benjamin
 *
 */
public class QueryParserTest {

    @Test
    public void testParseCreateDatabase() throws HorizonDBException {
       
        Msg<CreateDatabasePayload> msg = QueryParser.parse(newMsg(" CREATE DATABASE TEST;"));
        
        assertNotNull(msg);
        
        msg = QueryParser.parse(newMsg("CREATE DATABASE test ;"));
        
        assertNotNull(msg);
        assertEquals(new DatabaseDefinition("test"), msg.getPayload().getDefinition());
    }
    
    @Test
    public void testParseUseDatabase() throws HorizonDBException {
       
        Msg<UseDatabasePayload> msg = QueryParser.parse(newMsg("USE TEST;"));
        
        assertNotNull(msg);
        assertEquals("TEST", msg.getPayload().getDatabase());
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
        
        Msg<CreateTimeSeriesPayload> msg = QueryParser.parse(newMsg("CREATE TIMESERIES Dax (" +
                                        "Quote(received NANOSECONDS_TIMESTAMP, bidPrice DECIMAL, askPrice DECIMAL, bidVolume INTEGER, askVolume INTEGER), " +
                                        "Trade(received NANOSECONDS_TIMESTAMP, price DECIMAL, volume INTEGER))TIME_UNIT = NANOSECONDS TIMEZONE = 'Europe/Berlin';"));
        assertNotNull(msg);
        
        assertEquals(expected, msg.getPayload().getDefinition());
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
        
        Msg<CreateTimeSeriesPayload> msg = QueryParser.parse(newMsg("CREATE TIMESERIES Dax (" +
                                        "Quote(received NANOSECONDS_TIMESTAMP, bidPrice DECIMAL, askPrice DECIMAL, bidVolume INTEGER, askVolume INTEGER), " +
                                        "Trade(received NANOSECONDS_TIMESTAMP, price DECIMAL, volume INTEGER))TIMEZONE = 'America/Los_Angeles';"));
        assertNotNull(msg);
        
        assertEquals(expected, msg.getPayload().getDefinition());
    }
    
    @Test
    public void testParseSelectAll() throws HorizonDBException {
        
        Msg<SelectPayload> msg = QueryParser.parse(newMsg("SELECT * FROM Dax;"));
        assertEquals("Dax", msg.getPayload().getSeriesName());
        assertEquals("", msg.getPayload().getPredicate().toString());
    }
    
    @Test
    public void testParseSelectWithTimestampGreaterThan() throws HorizonDBException {

        testParseSelectWithPredicate("timestamp > 2");
    }
    
    @Test
    public void testParseSelectWithAndAndOr() throws HorizonDBException {

        testParseSelectWithPredicate("timestamp > 2 AND timestamp < 4 OR timestamp > 6 AND timestamp < 8");
    }
    
    @Test
    public void testParseSelectWithTimestampUnit() throws HorizonDBException {

        testParseSelectWithPredicate("timestamp >= 1384425960200ms AND timestamp < 1384425960400ms");
    }
    
    @Test
    public void testParseSelectWithAndOrAndParentheses() throws HorizonDBException {

        testParseSelectWithPredicate("timestamp > 2 AND (timestamp < 4 OR timestamp > 6)");
    }
    
    @Test
    public void testParseSelectWithInExpression() throws HorizonDBException {

        testParseSelectWithPredicate("timestamp IN (2, 4)");
    }
    
    @Test
    public void testParseSelectWithBetweenExpression() throws HorizonDBException {

        testParseSelectWithPredicate("timestamp BETWEEN 2 AND 4");
    }
    
    @Test
    public void testParseSelectWithNotBetweenExpression() throws HorizonDBException {

        testParseSelectWithPredicate("timestamp NOT BETWEEN 2 AND 6");
    }
    
    @Test
    public void testParseSelectWithBetweenAndInExpression() throws HorizonDBException {

        testParseSelectWithPredicate("timestamp BETWEEN 2 AND 6 AND timestamp IN (5, 6)");
    }
    
    @Test
    public void testParseSelectWithNotInExpression() throws HorizonDBException {

        testParseSelectWithPredicate("timestamp NOT IN (2, 4)");
    }
    
    @Test
    public void testParseSelectWithInExpressionAndOnlyOneValue() throws HorizonDBException {

        testParseSelectWithPredicate("timestamp IN (2)");
    }

    /**
     * Test that the parsing of a select statement with the specified predicate works.
     * 
     * @param predicate the predicate to test
     * @throws HorizonDBException if a problem occurs
     */
    private static void testParseSelectWithPredicate(String predicate) throws HorizonDBException {
        
        Msg<SelectPayload> msg = QueryParser.parse(newMsg("SELECT * FROM Dax WHERE " + predicate + ";"));
        assertEquals("Dax", msg.getPayload().getSeriesName());
        assertEquals(predicate, msg.getPayload().getPredicate().toString());
    }
    
//    @Test
//    public void testParseInsert() throws HorizonDBException {
//        
//        SelectQuery query = (SelectQuery) QueryParser.parse("INSERT INTO Dax.Trade (timestamp, price, volume) VALUES ('23-05-2014 09:44:30', 125E-1, 10);");
//    }
    
    @Test
    public void testParseWithInvalidQuery() throws HorizonDBException {
       
        try {
            
            QueryParser.parse(newMsg("CREATE DB TEST;"));
            fail();
            
        } catch (BadHqlGrammarException e) {
            assertTrue(true); 
        }
    }
    
    /**
     * Creates a new HQL query message for use within the tests.
     * 
     * @param query the HQL query
     * @return a new HQL query message for use within the tests.
     */
    private static Msg<HqlQueryPayload> newMsg(String query) {
        
        HqlQueryPayload payload = new HqlQueryPayload("TEST", query);
        return Msg.newRequestMsg(OpCode.HQL_QUERY, payload);
    }
}

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

import io.horizondb.db.Configuration;
import io.horizondb.db.HorizonDBException;
import io.horizondb.db.databases.Database;
import io.horizondb.db.databases.DatabaseManager;
import io.horizondb.db.databases.InMemoryDatabaseManager;
import io.horizondb.db.series.InMemoryTimeSeriesManager;
import io.horizondb.db.series.InMemoryTimeSeriesPartitionManager;
import io.horizondb.db.series.TimeSeriesManager;
import io.horizondb.db.series.TimeSeriesPartitionManager;
import io.horizondb.io.Buffer;
import io.horizondb.model.core.records.BinaryTimeSeriesRecord;
import io.horizondb.model.protocol.CreateDatabasePayload;
import io.horizondb.model.protocol.CreateTimeSeriesPayload;
import io.horizondb.model.protocol.HqlQueryPayload;
import io.horizondb.model.protocol.InsertPayload;
import io.horizondb.model.protocol.Msg;
import io.horizondb.model.protocol.OpCode;
import io.horizondb.model.protocol.SelectPayload;
import io.horizondb.model.protocol.UseDatabasePayload;
import io.horizondb.model.schema.DatabaseDefinition;
import io.horizondb.model.schema.RecordSetDefinition;
import io.horizondb.model.schema.RecordTypeDefinition;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static io.horizondb.test.AssertExceptions.assertErrorMessageContains;

import static io.horizondb.model.core.util.TimeUtils.EUROPE_BERLIN_TIMEZONE;
import static io.horizondb.model.core.util.TimeUtils.parseDateTime;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Benjamin
 *
 */
public class QueryParserTest {

    /**
     * The database configuration used during the tests
     */
    private Configuration configuration;
    
    /**
     * The database manager used during the tests
     */
    private DatabaseManager databaseManager;
    
    @Before
    public void setUp() throws IOException, InterruptedException {
        this.configuration = newConfiguration();
        this.databaseManager = newDatabaseManager(this.configuration);
        this.databaseManager.start();
    }
    
    @After
    public void tearDown() throws InterruptedException {
        this.databaseManager.shutdown();
        this.databaseManager = null;
        this.configuration = null;
    }
    
    @Test
    public void testParseCreateDatabase() throws HorizonDBException, IOException {
       
        Msg<CreateDatabasePayload> msg = QueryParser.parse(this.configuration, 
                                                           this.databaseManager,
                                                           newMsg("", " CREATE DATABASE TEST;"));
        
        assertNotNull(msg);
        
        msg = QueryParser.parse(this.configuration,
                                this.databaseManager,
                                newMsg("", "CREATE DATABASE test ;"));
        
        assertNotNull(msg);
        assertEquals(new DatabaseDefinition("test"), msg.getPayload().getDefinition());
    }
    
    @Test
    public void testParseUseDatabase() throws HorizonDBException, IOException  {
       
        Msg<UseDatabasePayload> msg = QueryParser.parse(this.configuration,
                                                        this.databaseManager,
                                                        newMsg("", "USE TEST;"));
        
        assertNotNull(msg);
        assertEquals("TEST", msg.getPayload().getDatabase());
    }
    
    @Test
    public void testParseCreateTimeSeries() throws HorizonDBException, IOException  {

        TimeSeriesDefinition expected = getTimeSeriesDefinition();
        
        createDatabase();
        
        Msg<CreateTimeSeriesPayload> msg = QueryParser.parse(this.configuration,
                                                             this.databaseManager,
                                                             newMsg("test", "CREATE TIMESERIES Dax (" +
                                        "Quote(bidPrice DECIMAL, askPrice DECIMAL, bidVolume INTEGER, askVolume INTEGER), " +
                                        "Trade(price DECIMAL, volume INTEGER))TIME_UNIT = NANOSECONDS TIMEZONE = 'Europe/Berlin';"));
        assertNotNull(msg);
        
        assertEquals(expected, msg.getPayload().getDefinition());
    }

    
    @Test
    public void testParseCreateTimeSeriesWithDefaultTimeUnit() throws HorizonDBException, IOException  {

        createDatabase();
        
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
        
        Msg<CreateTimeSeriesPayload> msg = QueryParser.parse(this.configuration,
                                                             this.databaseManager,
                                                             newMsg("test", "CREATE TIMESERIES Dax (" +
                                        "Quote(received NANOSECONDS_TIMESTAMP, bidPrice DECIMAL, askPrice DECIMAL, bidVolume INTEGER, askVolume INTEGER), " +
                                        "Trade(received NANOSECONDS_TIMESTAMP, price DECIMAL, volume INTEGER))TIMEZONE = 'America/Los_Angeles';"));
        assertNotNull(msg);
        
        assertEquals(expected, msg.getPayload().getDefinition());
    }
    
    @Test
    public void testParseSelectAll() throws HorizonDBException, IOException  {
        
        createDatabaseAndTimeSeries();
        
        Msg<SelectPayload> msg = QueryParser.parse(this.configuration,
                                                   this.databaseManager,
                                                   newMsg("test", "SELECT * FROM Dax;"));
        assertEquals("Dax", msg.getPayload().getSeriesName());
        assertEquals("", msg.getPayload().getPredicate().toString());
    }
    
    @Test
    public void testParseSelectWithTimestampGreaterThan() throws HorizonDBException, IOException  {

        testParseSelectWithPredicate("timestamp > 2");
    }
    
    @Test
    public void testParseSelectWithAndAndOr() throws HorizonDBException, IOException  {

        testParseSelectWithPredicate("timestamp > 2 AND timestamp < 4 OR timestamp > 6 AND timestamp < 8");
    }
    
    @Test
    public void testParseSelectWithTimestampUnit() throws HorizonDBException, IOException  {

        testParseSelectWithPredicate("timestamp >= 1384425960200ms AND timestamp < 1384425960400ms");
    }
    
    @Test
    public void testParseSelectWithAndOrAndParentheses() throws HorizonDBException, IOException  {

        testParseSelectWithPredicate("timestamp > 2 AND (timestamp < 4 OR timestamp > 6)");
    }
    
    @Test
    public void testParseSelectWithInExpression() throws HorizonDBException, IOException  {

        testParseSelectWithPredicate("timestamp IN (2, 4)");
    }
    
    @Test
    public void testParseSelectWithBetweenExpression() throws HorizonDBException, IOException  {

        testParseSelectWithPredicate("timestamp BETWEEN 2 AND 4");
    }
    
    @Test
    public void testParseSelectWithNotBetweenExpression() throws HorizonDBException, IOException  {

        testParseSelectWithPredicate("timestamp NOT BETWEEN 2 AND 6");
    }
    
    @Test
    public void testParseSelectWithBetweenAndInExpression() throws HorizonDBException, IOException  {

        testParseSelectWithPredicate("timestamp BETWEEN 2 AND 6 AND timestamp IN (5, 6)");
    }
    
    @Test
    public void testParseSelectWithNotInExpression() throws HorizonDBException, IOException  {

        testParseSelectWithPredicate("timestamp NOT IN (2, 4)");
    }
    
    @Test
    public void testParseSelectWithInExpressionAndOnlyOneValue() throws HorizonDBException, IOException  {

        testParseSelectWithPredicate("timestamp IN (2)");
    }
    
    @Test
    public void testParseInsert() throws HorizonDBException, IOException  {
        
        createDatabaseAndTimeSeries();
        
        Msg<InsertPayload> msg = QueryParser.parse(this.configuration,
                                                   this.databaseManager,
                                                   newMsg("test", "INSERT INTO Dax.Trade (timestamp, price, volume) VALUES ('2014-05-23 09:44:30', 125E-1, 10);"));
        
        InsertPayload payload = msg.getPayload();
        assertEquals("Dax", payload.getSeries());
        assertEquals(1, payload.getRecordType());
        
        TimeSeriesDefinition definition = getTimeSeriesDefinition();
        Buffer buffer = payload.getBuffer(); 
        BinaryTimeSeriesRecord binaryRecord = definition.newBinaryRecord(payload.getRecordType());
        binaryRecord.fill(buffer);
        assertEquals(parseDateTime(EUROPE_BERLIN_TIMEZONE, "2014-05-23 09:44:30"), binaryRecord.getTimestampInMillis(0));
        assertEquals(12.5, binaryRecord.getDouble(1), 0.0);
        assertEquals(10L, binaryRecord.getLong(2));
    }
    
    @Test
    public void testParseInsertWithoutFieldNames() throws HorizonDBException, IOException  {
        
        createDatabaseAndTimeSeries();
        
        Msg<InsertPayload> msg = QueryParser.parse(this.configuration,
                                                   this.databaseManager,
                                                   newMsg("test", "INSERT INTO Dax.Trade VALUES ('2014-05-23 09:44:30', 125E-1, 10);"));
        
        InsertPayload payload = msg.getPayload();
        assertEquals("Dax", payload.getSeries());
        assertEquals(1, payload.getRecordType());
        
        TimeSeriesDefinition definition = getTimeSeriesDefinition();
        Buffer buffer = payload.getBuffer(); 
        BinaryTimeSeriesRecord binaryRecord = definition.newBinaryRecord(payload.getRecordType());
        binaryRecord.fill(buffer);
        assertEquals(parseDateTime(EUROPE_BERLIN_TIMEZONE, "2014-05-23 09:44:30"), binaryRecord.getTimestampInMillis(0));
        assertEquals(12.5, binaryRecord.getDouble(1), 0.0);
        assertEquals(10L, binaryRecord.getLong(2));
    }
    
    @Test
    public void testParseInsertWithInvalidTimeValue() throws HorizonDBException, IOException  {
        
        createDatabaseAndTimeSeries();
        
        try {
        
            QueryParser.parse(this.configuration,
                              this.databaseManager,
                              newMsg("test",
                                     "INSERT INTO Dax.Trade (timestamp, price, volume) VALUES ('2014.05.23 09:44:30', 125E-1, 10);"));
            fail();
        } catch (BadHqlGrammarException e) {
            String msgFragment = "The value 2014.05.23 09:44:30 cannot be parsed into a valid date";
            assertErrorMessageContains(msgFragment, e);
        }
    }

    @Test
    public void testParseInsertWithInvalidLongValue() throws HorizonDBException, IOException {

        createDatabaseAndTimeSeries();

        try {

            QueryParser.parse(this.configuration,
                              this.databaseManager,
                              newMsg("test",
                                     "INSERT INTO Dax.Trade (timestamp, price, volume) VALUES ('2014-05-23 09:44:30', 125E-1, 'test');"));
            fail();
        } catch (BadHqlGrammarException e) {
            String msgFragment = "The value 'test' cannot be converted into a number";
            assertErrorMessageContains(msgFragment, e);
        }
    }

    @Test
    public void testParseWithInvalidQuery() throws HorizonDBException, IOException  {
       
        try {
            
            Configuration configuration = newConfiguration();
            QueryParser.parse(configuration,
                              newDatabaseManager(configuration),
                              newMsg("", "CREATE DB TEST;"));
            fail();
            
        } catch (BadHqlGrammarException e) {
            assertTrue(true); 
        }
    }

    @Test
    public void testParseSelectWithOneRecordProjection() throws HorizonDBException, IOException {

        createDatabaseAndTimeSeries();
        
        TimeSeriesDefinition timeSeriesDefinition = getTimeSeriesDefinition();
        
        Msg<SelectPayload> msg = QueryParser.parse(this.configuration,
                                                   this.databaseManager,
                                                   newMsg("test", "SELECT Trade.* FROM Dax;"));
        assertEquals("Dax", msg.getPayload().getSeriesName());
        
        RecordSetDefinition definition = msg.getPayload().getProjection().getDefinition(timeSeriesDefinition);
        assertEquals(1, definition.getNumberOfRecordTypes());
        assertEquals(timeSeriesDefinition.getRecordType(1), definition.getRecordType(0));
    }

    /**
     * Creates a new HQL query message for use within the tests.
     * 
     * @param database the database name
     * @param query the HQL query
     * @return a new HQL query message for use within the tests.
     */
    private static Msg<HqlQueryPayload> newMsg(String database, String query) throws IOException  {
        
        HqlQueryPayload payload = new HqlQueryPayload(database, query);
        return Msg.newRequestMsg(OpCode.HQL_QUERY, payload);
    }
    
    /**
     * Test that the parsing of a select statement with the specified predicate works.
     * 
     * @param predicate the predicate to test
     * @throws HorizonDBException if a problem occurs
     */
    private void testParseSelectWithPredicate(String predicate) throws HorizonDBException, IOException  {
        
        createDatabaseAndTimeSeries();
        
        Msg<SelectPayload> msg = QueryParser.parse(this.configuration,
                                                   this.databaseManager,
                                                   newMsg("test", "SELECT * FROM Dax WHERE " + predicate + ";"));
        assertEquals("Dax", msg.getPayload().getSeriesName());
        assertEquals(predicate, msg.getPayload().getPredicate().toString());
    }
    
    /**
     * Creates a new configuration instance.
     * @return a new configuration instance.
     */
    private static Configuration newConfiguration() {
        return Configuration.newBuilder().build();
    }
   

    /**
     * Creates a new database with a time series.
     */
    private void createDatabaseAndTimeSeries() throws IOException, HorizonDBException {
        createDatabase();
        Database database = this.databaseManager.getDatabase("test");
        database.createTimeSeries(getTimeSeriesDefinition(), true);
    }

    /**
     * Creates a new database
     * @throws IOException if an I/O problem occurs while creating the database.
     * @throws HorizonDBException if the database cannot be created
     */
    private void createDatabase() throws IOException, HorizonDBException {
        this.databaseManager.createDatabase(new DatabaseDefinition("test"), true);
    }
    
    /**
     * Creates a new <code>DatabaseManager</code> stub.
     * @param configuration the database configuration
     * @return a new <code>DatabaseManager</code> stub.
     */
    private static DatabaseManager newDatabaseManager(Configuration configuration) {

        TimeSeriesPartitionManager partitionManager = new InMemoryTimeSeriesPartitionManager(configuration);
        TimeSeriesManager timeSeriesManager = new InMemoryTimeSeriesManager(partitionManager, configuration);
        return new InMemoryDatabaseManager(configuration, timeSeriesManager);
    }

    /**
     * Returns the definition of the time series used within the tests 
     * @return the definition of the time series used by within tests 
     */
    private static TimeSeriesDefinition getTimeSeriesDefinition() {
        RecordTypeDefinition quote = RecordTypeDefinition.newBuilder("Quote")
                                                         .addDecimalField("bidPrice")
                                                         .addDecimalField("askPrice")
                                                         .addIntegerField("bidVolume")
                                                         .addIntegerField("askVolume")
                                                         .build();

        RecordTypeDefinition trade = RecordTypeDefinition.newBuilder("Trade")
                                                         .addDecimalField("price")
                                                         .addIntegerField("volume")
                                                         .build();

        TimeSeriesDefinition expected = TimeSeriesDefinition.newBuilder("Dax")
                                                            .addRecordType(quote)
                                                            .addRecordType(trade)
                                                            .timeUnit(TimeUnit.NANOSECONDS)
                                                            .timeZone(TimeZone.getTimeZone("Europe/Berlin"))
                                                            .build();
        return expected;
    }
}



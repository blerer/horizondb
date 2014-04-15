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
import io.horizondb.db.Query;
import io.horizondb.db.queries.CreateDatabaseQuery;
import io.horizondb.db.queries.CreateTimeSeriesQuery;
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

        TimeSeriesDefinition definition = TimeSeriesDefinition.newBuilder("Dax")
                                                              .addRecordType(quote)
                                                              .addRecordType(trade)
                                                              .build();
        
        Query query = QueryParser.parse(definition.toHql());
        
        assertNotNull(query);
        
        assertEquals(definition, ((CreateTimeSeriesQuery) query).getDefinition());
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

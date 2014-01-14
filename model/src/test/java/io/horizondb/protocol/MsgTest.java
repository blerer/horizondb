/**
 * Copyright 2013 Benjamin Lerer
 * 
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
package io.horizondb.protocol;

import io.horizondb.io.Buffer;
import io.horizondb.io.buffers.Buffers;
import io.horizondb.model.DatabaseDefinition;
import io.horizondb.model.FieldType;
import io.horizondb.model.RecordTypeDefinition;
import io.horizondb.model.TimeSeriesDefinition;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Benjamin
 *
 */
public class MsgTest {

	@Test
	public void testComputeSize() throws IOException {

		RecordTypeDefinition quote = RecordTypeDefinition.newBuilder("Quote")
                .addField("bestBid", FieldType.DECIMAL)
                .addField("bestAsk", FieldType.DECIMAL)
                .addField("bidVolume", FieldType.INTEGER)
                .addField("askVolume", FieldType.INTEGER)
                .build();

		RecordTypeDefinition trade = RecordTypeDefinition.newBuilder("Trade")
                .addField("price", FieldType.DECIMAL)
                .addField("volume", FieldType.DECIMAL)
                .addField("aggressorSide", FieldType.BYTE)
                .build();
		
		DatabaseDefinition databaseDefinition = new DatabaseDefinition("test");
		
		TimeSeriesDefinition definition = databaseDefinition.newTimeSeriesDefinitionBuilder("DAX")
				         .timeUnit(TimeUnit.NANOSECONDS)
	                     .addRecordType(quote)
	                     .addRecordType(trade)
	                     .build();

		Msg<TimeSeriesDefinition> msg = Msg.newRequestMsg(OpCode.CREATE_TIMESERIES, definition);
		
		Buffer buffer = Buffers.allocate(200);
		
		msg.writeTo(buffer);
		
		assertEquals(buffer.readableBytes(), msg.computeSerializedSize());
	}
}

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

import java.io.IOException;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Benjamin
 *
 */
public class MsgHeaderTest {

	@Test
	public void testComputeSize() throws IOException {
		
		MsgHeader header = MsgHeader.newRequestHeader(OpCode.CREATE_TIMESERIES, 124);
		
		Buffer buffer = Buffers.allocate(100);
		
		header.writeTo(buffer);
		
		assertEquals(buffer.readableBytes(), header.computeSerializedSize());
	}
	
	@Test
	public void testGetParser() throws IOException {
		
		MsgHeader header = MsgHeader.newRequestHeader(OpCode.CREATE_TIMESERIES, 124);
		
		Buffer buffer = Buffers.allocate(100);
		
		header.writeTo(buffer);

		MsgHeader deserializedHeader = MsgHeader.getParser().parseFrom(buffer);
		assertEquals(header, deserializedHeader);
	}

}

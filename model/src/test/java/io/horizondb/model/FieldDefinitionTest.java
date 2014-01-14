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
package io.horizondb.model;

import java.io.IOException;

import io.horizondb.io.Buffer;
import io.horizondb.io.buffers.Buffers;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author Benjamin
 *
 */
public class FieldDefinitionTest {

	@Test
	public void testGetParser() throws IOException {
		
		FieldDefinition definition = FieldDefinition.newInstance("volume", FieldType.INTEGER);
		
		Buffer buffer = Buffers.allocate(100);
		definition.writeTo(buffer);
		
		FieldDefinition deserializedDefinition = FieldDefinition.getParser().parseFrom(buffer);
		assertEquals(definition, deserializedDefinition);
	}
	
	@Test
	public void testParseFrom() throws IOException {
		
		FieldDefinition definition = FieldDefinition.newInstance("volume", FieldType.INTEGER);
		
		Buffer buffer = Buffers.allocate(100);
		definition.writeTo(buffer);
		
		FieldDefinition deserializedDefinition = FieldDefinition.parseFrom(buffer);
		assertEquals(definition, deserializedDefinition);
	}

	@Test
	public void testComputeSerializedSize() throws IOException {
		
		FieldDefinition definition = FieldDefinition.newInstance("volume", FieldType.INTEGER);
		
		Buffer buffer = Buffers.allocate(100);
		definition.writeTo(buffer);
		
		assertEquals(buffer.readableBytes(), definition.computeSerializedSize());
	}

}

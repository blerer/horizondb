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
package io.horizondb.io.checksum;

import io.horizondb.io.buffers.Buffers;

import java.util.Random;
import java.util.zip.CRC32;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Unit test inspired by the one of PureJavaCrc32 of HADOOP. 
 *  
 * @author benjamin
 *
 */
public class Crc32Test {
    
    /**
     * The class under test
     */
    private Crc32 crc;
    
    /**
     * The JDK CRC implementation used as a reference.
     */
    private CRC32 jdkCrc;
    
    private Random random = new Random();

    @Before
    public void setUp() {
      this.jdkCrc = new CRC32();
      this.crc = new Crc32(); 
    }

    @After
    public void tearDown() {
      this.jdkCrc = null;
      this.crc = null; 
    }

    @Test
    public void testWithNoUpdate() throws Exception {

        assertEquals(this.jdkCrc.getValue(), this.crc.getValue());
    }
    
    @Test
    public void testUpdateWithByte() throws Exception {

        this.crc.update(104);
        this.jdkCrc.update(104);
        assertEquals(this.jdkCrc.getValue(), this.crc.getValue());

        assertCrcEqualsWhenUpdatedByteByByte(new byte[] { 40, 60, 97, -70 });
        assertCrcEqualsWhenUpdatedByteByByte("hello world!".getBytes("UTF-8"));

        for (int i = 0; i < 24; i++) {
            byte randomBytes[] = new byte[new Random().nextInt(2048)];
            this.random.nextBytes(randomBytes);
            assertCrcEqualsWhenUpdatedByteByByte(randomBytes);
        }
    }
    
    @Test
    public void testUpdateWithBuffer() throws Exception {

        assertCrcEqualsWhenUpdatedWithBuffer(new byte[] { 40, 60, 97, -70 });
        assertCrcEqualsWhenUpdatedWithBuffer("hello world!".getBytes("UTF-8"));

        for (int i = 0; i < 24; i++) {
            byte randomBytes[] = new byte[new Random().nextInt(2048)];
            this.random.nextBytes(randomBytes);
            assertCrcEqualsWhenUpdatedWithBuffer(randomBytes);
        }
    }
    
    @Test
    public void testUpdateWithByteArray() throws Exception {

    	assertCrcEqualsWhenUpdatedWithByteArray(new byte[] { 40, 60, 97, -70 });
    	assertCrcEqualsWhenUpdatedWithByteArray("hello world!".getBytes("UTF-8"));

        for (int i = 0; i < 24; i++) {
            byte randomBytes[] = new byte[new Random().nextInt(2048)];
            this.random.nextBytes(randomBytes);
            assertCrcEqualsWhenUpdatedWithByteArray(randomBytes);
        }
    }
    
    /**
     * Checks that the CRCs are equals when updated byte by byte. 
     * 
     * @param bytes the bytes which should be used to update the CRCs.
     */
    private void assertCrcEqualsWhenUpdatedByteByByte(byte[] bytes) {

        this.crc.reset();
        this.jdkCrc.reset();

        assertEquals(this.jdkCrc.getValue(), this.crc.getValue());

        for (int i = 0; i < bytes.length; i++) {
            this.crc.update(bytes[i]);
            this.jdkCrc.update(bytes[i]);

            assertEquals(this.jdkCrc.getValue(), this.crc.getValue());
        }
    }
    
    /**
     * Checks that the CRCs are equals when updated with a buffer. 
     * 
     * @param bytes the bytes which should be used to update the CRCs.
     */
    private void assertCrcEqualsWhenUpdatedWithBuffer(byte[] bytes) {

        this.crc.reset();
        this.jdkCrc.reset();

        assertEquals(this.jdkCrc.getValue(), this.crc.getValue());

        this.crc.update(Buffers.wrap(bytes));
        this.jdkCrc.update(bytes);

        assertEquals(this.jdkCrc.getValue(), this.crc.getValue());
    }
    
    /**
     * Checks that the CRCs are equals when updated with a byte array. 
     * 
     * @param bytes the bytes which should be used to update the CRCs.
     */
    private void assertCrcEqualsWhenUpdatedWithByteArray(byte[] bytes) {

        this.crc.reset();
        this.jdkCrc.reset();

        assertEquals(this.jdkCrc.getValue(), this.crc.getValue());

        this.crc.update(bytes);
        this.jdkCrc.update(bytes);

        assertEquals(this.jdkCrc.getValue(), this.crc.getValue());
    }
  }
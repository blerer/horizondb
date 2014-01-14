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
package io.horizondb.io.encoding;

import io.horizondb.io.ByteReader;
import io.horizondb.io.ByteWriter;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * Utility class for working with the varint encoding used by Google Protocol Buffers.   
 * 
 * @see http://code.google.com/apis/protocolbuffers/docs/encoding.html
 * 
 * @author Benjamin
 *
 */
public final class VarInts {

    /**
     * Read the next <code>byte</code> available from the specified <code>ByteReader</code>.
     * 
     * @param reader the <code>ByteReader</code> to read from.
     * @return the next <code>byte</code> available from the specified <code>ByteReader</code>.
     * @throws IOException if an I/O problem occurs.
     */
    public static int readByte(ByteReader reader) throws IOException {

        return reader.readByte();
    }
	
    /**
     * Read an <code>int</code> from the specified <code>ByteReader</code>.
     * 
     * @param reader the <code>ByteReader</code> to read from.
     * @return the int value corresponding to the next bytes from the specified <code>ByteReader</code>.
     * @throws IOException if an I/O problem occurs.
     */
    public static int readInt(ByteReader reader) throws IOException {

        return decodeZigZag32(readUnsignedInt(reader));
    }

    /**
     * Writes the specified int to the specified <code>ByteWriter</code>.
     * 
     * @param writer the <code>ByteWriter</code> to write to.
     * @param i the int to write.
     * @throws IOException if a problem occurs while writing to the output.
     */
    public static void writeInt(ByteWriter writer, int i) throws IOException {

        writeUnsignedInt(writer, encodeZigZag32(i));
    }

    /**
     * Writes the specified long to the specified <code>ByteWriter</code>.
     * 
     * @param writer the <code>ByteWriter</code> to write to.
     * @param l the long to write.
     * @throws IOException if a problem occurs while writing to the output.
     */
    public static void writeLong(ByteWriter writer, long l) throws IOException {

        writeUnsignedLong(writer, encodeZigZag64(l));
    }

    /**
     * Computes the number of bytes needed to serialize the specified <code>String</code>.
     * 
     * @param s the <code>String</code>
     * @return the number of bytes needed to serialize the specified <code>String</code>.
     */
    public static int computeStringSize(String s) {
    	
        try {
	                	
        	byte[] bytes = s.getBytes("UTF-8");
        	
        	return computeUnsignedIntSize(bytes.length) + bytes.length;
	        
        } catch (UnsupportedEncodingException e) {
	        throw new IllegalStateException(e);
        } 
    }
    
    /**
     * Computes the number of bytes needed to serialize the specified signed <code>int</code>.
     * 
     * @param i the <code>int</code>
     * @return the number of bytes needed to serialize the specified signed <code>int</code>.
     */
	public static int computeIntSize(int i) {
		
		return computeUnsignedIntSize(encodeZigZag32(i));
	}
    
    /**
     * Computes the number of bytes needed to serialize the specified unsigned <code>int</code>.
     * 
     * @param i the <code>int</code>
     * @return the number of bytes needed to serialize the specified unsigned <code>int</code>.
     */
	public static int computeUnsignedIntSize(int i) {

		if ((i & (0xffffffff << 7)) == 0) {
			return 1;
		}

		if ((i & (0xffffffff << 14)) == 0) {
			return 2;
		}
		
		if ((i & (0xffffffff << 21)) == 0) {
			return 3;
		}	
		
		if ((i & (0xffffffff << 28)) == 0) {
			return 4;
		}
		
		return 5;
	}
	
    /**
     * Computes the number of bytes needed to serialize the specified signed <code>long</code>.
     * 
     * @param l the <code>long</code>
     * @return the number of bytes needed to serialize the specified signed <code>long</code>.
     */
	public static int computeLongSize(long l) {
		
		return computeUnsignedLongSize(encodeZigZag64(l));
	}

    /**
     * Computes the number of bytes needed to serialize the specified unsigned <code>long</code>.
     * 
     * @param l the <code>long</code>
     * @return the number of bytes needed to serialize the specified unsigned <code>long</code>.
     */
	public static int computeUnsignedLongSize(long l) {

		if ((l & (0xffffffffffffffffL << 7)) == 0) {
			return 1;
		}
		
		if ((l & (0xffffffffffffffffL << 14)) == 0) {
			return 2;
		}
		
		if ((l & (0xffffffffffffffffL << 21)) == 0) {
			return 3;
		}
		
		if ((l & (0xffffffffffffffffL << 28)) == 0) {
			return 4;
		}
		
		if ((l & (0xffffffffffffffffL << 35)) == 0) {
			return 5;
		}
		
		if ((l & (0xffffffffffffffffL << 42)) == 0) {
			return 6;
		}
		
		if ((l & (0xffffffffffffffffL << 49)) == 0) {
			return 7;
		}
		
		if ((l & (0xffffffffffffffffL << 56)) == 0) {
			return 8;
		}
		
		if ((l & (0xffffffffffffffffL << 63)) == 0) {
			return 9;
		}

		return 10;
	}
    
    /**
     * Writes the specified <code>String</code> to the specified <code>ByteWriter</code>.
     * 
     * @param writer the <code>ByteWriter</code> to write to.
     * @param s the <code>String</code> to write.
     * @throws IOException if an I/O problem occurs.
     */
	public static void writeString(ByteWriter writer, String s) throws IOException {

		byte[] bytes = s.getBytes("UTF-8");
		writeUnsignedInt(writer, bytes.length);

		for (byte b : bytes) {
			writer.writeByte(b);
		}
	}
    
    /**
     * Writes the specified <code>byte</code> to the specified <code>ByteWriter</code>.
     * 
     * @param writer the <code>ByteWriter</code> to write to.
     * @param b the <code>byte</code> to write.
     * @throws IOException if an I/O problem occurs.
     */
	public static void writeByte(ByteWriter writer, int b) throws IOException {
		
		writer.writeByte(b);
	}
	
    /**
     * Writes the specified unsigned int to the specified <code>ByteWriter</code>.
     * 
     * @param writer the <code>ByteWriter</code> to write to.
     * @param i the int to write.
     * @throws IOException if an I/O problem occurs.
     */
    public static void writeUnsignedInt(ByteWriter writer, int i) throws IOException {

        int value = i;

        while (true) {
            
            if ((value & ~0x7F) == 0) {
            	writer.writeByte((byte) value);
                return;
            }

            writer.writeByte((byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
    }
    
    /**
     * Write the specified unsigned long to the specified <code>ByteWriter</code>.
     * 
     * @param writer the <code>ByteWriter</code> to write to.
     * @param l the long to write.
     * @throws IOException if a problem occurs while writing to the <code>ByteWriter</code>.
     */
    public static void writeUnsignedLong(ByteWriter writer, long l) throws IOException {

        long value = l;

        while (true) {
            
            if ((value & ~0x7FL) == 0) {
            	writer.writeByte((byte) value);
                return;
            }

            writer.writeByte((byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
    }

    /**
     * Read a <code>long</code> from the specified <code>ByteReader</code>.
     * 
     * @param reader the <code>ByteReader</code> to read from.
     * @return the long value corresponding to the next bytes.
     * @throws IOException if a problem occurs while reading form the <code>ByteReader</code>.
     */
    public static long readLong(ByteReader reader) throws IOException {

        return decodeZigZag64(readUnsignedLong(reader));
    }

    /**
     * Read a <code>String</code> from the specified <code>ByteReader</code>.
     * 
     * @param reader the <code>ByteReader</code> to read from.
     * @return the <code>String</code> value corresponding to the next bytes.
     * @throws IOException if a problem occurs while reading form the input.
     */
    public static String readString(ByteReader reader) throws IOException {

        byte[] bytes = new byte[readUnsignedInt(reader)];

        reader.readBytes(bytes);

        return new String(bytes, "UTF-8");
    }

    /**
     * Read an unsigned <code>int</code> from the specified <code>ByteReader</code>.
     * 
     * @param reader the <code>ByteReader</code> to read from.
     * @return the unsigned int corresponding to the next bytes.
     * @throws IOException if a problem occurs while reading form the input.
     */
    public static int readUnsignedInt(ByteReader reader) throws IOException {

        int shift = 0;
        int result = 0;

        while (shift < 32) {
            final byte b = reader.readByte();
            result |= getLower7Bits(b) << shift;

            if (isLastByte(b)) {
                return result;
            }

            shift += 7;
        }

        throw new IllegalStateException("Malformed varint32");
    }

    /**
     * Read an unsigned <code>long</code> from the specified <code>ByteReader</code>.
     * 
     * @param reader the <code>ByteReader</code> to read from.
     * @return the unsigned long corresponding to the next bytes.
     * @throws IOException if a problem occurs while reading form the <code>ByteReader</code>.
     */
    public static long readUnsignedLong(ByteReader reader) throws IOException {

        int shift = 0;
        long result = 0;

        while (shift < 64) {
            final byte b = reader.readByte();
            result |= (long) getLower7Bits(b) << shift;

            if (isLastByte(b)) {
                return result;
            }

            shift += 7;
        }

        throw new IllegalStateException("Malformed varint64");
    }

    /**
     * Skips the bytes corresponding to the next <code>int</code> from the specified <code>ByteReader</code>.
     * 
     * @param reader the <code>ByteReader</code> to read from.
     * @throws IOException if an I/O problem occurs.
     */
    public static void skipInt(ByteReader reader) throws IOException {

        skipUnsignedInt(reader);
    }

    /**
     * Skips the bytes corresponding to the next <code>long</code> from the specified <code>ByteReader</code>.
     * 
     * @param reader the <code>ByteReader</code> to read from.
     * @throws IOException if an I/O problem occurs.
     */
    public static void skipLong(ByteReader reader) throws IOException {

        skipUnsignedLong(reader);
    }

    /**
     * Skips the bytes corresponding to the next unsigned <code>long</code> from the specified <code>ByteReader</code>.
     * 
     * @param reader the <code>ByteReader</code> to read from.
     * @throws IOException if an I/O problem occurs.
     */
    public static void skipUnsignedLong(ByteReader reader) throws IOException {

        int shift = 0;

        while (shift < 64) {
            final byte b = reader.readByte();

            if (isLastByte(b)) {
                return;
            }

            shift += 7;
        }

        throw new IllegalStateException("Malformed varint64");
    }

    /**
     * Skips the bytes corresponding to the next unsigned <code>int</code> from the specified <code>ByteReader</code>.
     * 
     * @param reader the <code>ByteReader</code> to read from.
     * @throws IOException if an I/O problem occurs.
     */
    public static void skipUnsignedInt(ByteReader reader) throws IOException {

        int myShift = 0;

        while (myShift < 32) {
            final byte b = reader.readByte();

            if (isLastByte(b)) {
                return;
            }

            myShift += 7;
        }

        throw new IllegalStateException("Malformed varint32");
    }

    /**
     * Decode a ZigZag-encoded 32-bit value.  ZigZag encodes signed integers
     * into values that can be efficiently encoded with varint.  (Otherwise,
     * negative values must be sign-extended to 64 bits to be varint encoded,
     * thus always taking 10 bytes on the wire.)
     *
     * <p>This code has been copied from 
     * <code>com.google.protobuf.CodedInputStream</code>.</p>
     *
     * @param n An unsigned 32-bit integer, stored in a signed int because
     *          Java has no explicit unsigned support.
     * @return A signed 32-bit integer.
     */
    private static int decodeZigZag32(int n) {

        return (n >>> 1) ^ -(n & 1);
    }

    /**
     * Decode a ZigZag-encoded 64-bit value.  ZigZag encodes signed integers
     * into values that can be efficiently encoded with varint.  (Otherwise,
     * negative values must be sign-extended to 64 bits to be varint encoded,
     * thus always taking 10 bytes on the wire.)
     * 
     * <p>This code has been copied from 
     * <code>com.google.protobuf.CodedInputStream</code>.</p>
     *
     * @param n An unsigned 64-bit integer, stored in a signed int because
     *          Java has no explicit unsigned support.
     * @return A signed 64-bit integer.
     */
    private static long decodeZigZag64(long n) {

        return (n >>> 1) ^ -(n & 1);
    }

    /**
     * Encode a ZigZag-encoded 32-bit value.  ZigZag encodes signed integers
     * into values that can be efficiently encoded with varint.  (Otherwise,
     * negative values must be sign-extended to 64 bits to be varint encoded,
     * thus always taking 10 bytes on the wire.)
     *
     * <p>This code has been copied from 
     * <code>com.google.protobuf.CodedOutputStream</code>.</p>
     *
     * @param n A signed 32-bit integer.
     * @return An unsigned 32-bit integer, stored in a signed int because
     *         Java has no explicit unsigned support.
     */
    private static int encodeZigZag32(int n) {
        
      // Note:  the right-shift must be arithmetic
      return (n << 1) ^ (n >> 31);
    }
    
    /**
     * Encode a ZigZag-encoded 64-bit value.  ZigZag encodes signed integers
     * into values that can be efficiently encoded with varint.  (Otherwise,
     * negative values must be sign-extended to 64 bits to be varint encoded,
     * thus always taking 10 bytes on the wire.)
     *
     * <p>This code has been copied from 
     * <code>com.google.protobuf.CodedOutputStream</code>.</p>
     *
     * @param n A signed 64-bit integer.
     * @return An unsigned 64-bit integer, stored in a signed int because
     *         Java has no explicit unsigned support.
     */
    private static long encodeZigZag64(long n) {
        
      // Note:  the right-shift must be arithmetic
      return (n << 1) ^ (n >> 63);
    }
    
    /**
     * Returns the lower 7 bits of the specified byte.
     * 
     * @param b the byte 
     * @return the lower 7 bits of the specified byte.
     */
    private static int getLower7Bits(byte b) {

        return b & 0x7F;
    }

    /**
     * Returns <code>true</code> if there are no further bytes to come after 
     * that byte.
     * <p>Each byte in a varint, except the last byte, has the most significant
     *  bit (msb)  </p>
     * 
     * @return <code>true</code> if there are no further bytes to come after 
     * that byte.
     */
    private static boolean isLastByte(byte b) {

        return (b & 0x80) == 0;
    }

    /**
     * This class must not be instantiated.
     */
    private VarInts() {
        
    }

}

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
package io.horizondb.io.files;

import io.horizondb.io.ByteReader;
import io.horizondb.io.ReadableBuffer;
import io.horizondb.io.buffers.Buffers;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteOrder;

/**
 * @author Benjamin
 *
 */
public final class SeekableFileDataInputs {

	/**
	 * The empty input.
	 */
	private static final SeekableFileDataInput EMPTY_INPUT = new SeekableFileDataInput() {
		
		/**
		 * {@inheritDoc}
		 */
		@Override
		public ReadableBuffer slice(int length) throws IOException {
			
			if (length == 0) {
				
				return Buffers.EMPTY_BUFFER;
			}
			
			throw new EOFException();
		}
		
		/**		
		 * {@inheritDoc}
		 */
		@Override
        public long readableBytes() {
	        return 0;
        }

		/**
		 * {@inheritDoc}
		 */
		@Override
		public ByteReader skipBytes(int numberOfBytes) throws IOException {

			if (numberOfBytes == 0) {
				
				return this;
			}
			
			throw new EOFException();
		}
		
		/**
		 * {@inheritDoc}
		 */
		@Override
		public int readUnsignedShort() throws IOException {
			throw new EOFException();
		}
		
		/**
		 * {@inheritDoc}
		 */
		@Override
		public long readUnsignedInt() throws IOException {
			throw new EOFException();
		}
		
		/**
		 * {@inheritDoc}
		 */
		@Override
		public short readShort() throws IOException {
			throw new EOFException();
		}
		
		/**
		 * {@inheritDoc}
		 */
		@Override
		public long readLong() throws IOException {
			throw new EOFException();
		}
		
		/**
		 * {@inheritDoc}
		 */
		@Override
		public int readInt() throws IOException {
			throw new EOFException();
		}
		
		/**
		 * {@inheritDoc}
		 */
		@Override
		public ByteReader readBytes(byte[] bytes, int offset, int length) throws IOException {
			if (length == 0) {
				
				return this;
			}
			
			throw new EOFException();
		}
		
		/**
		 * {@inheritDoc}
		 */
		@Override
		public ByteReader readBytes(byte[] bytes) throws IOException {
			if (bytes.length == 0) {
				
				return this;
			}
			
			throw new EOFException();
		}
		
		/**
		 * {@inheritDoc}
		 */
		@Override
		public byte readByte() throws IOException {
			throw new EOFException();
		}
		
		/**
		 * {@inheritDoc}
		 */
		@Override
		public boolean readBoolean() throws IOException {
			throw new EOFException();
		}
		
		/**
		 * {@inheritDoc}
		 */
		@Override
		public ByteReader order(ByteOrder order) {
			return this;
		}
		
		/**
		 * {@inheritDoc}
		 */
		@Override
		public ByteOrder order() {
			return ByteOrder.nativeOrder();
		}
		
		/**
		 * {@inheritDoc}
		 */
		@Override
		public boolean isReadable() throws IOException {
			return false;
		}
		
		/**
		 * {@inheritDoc}
		 */
		@Override
		public void close() throws IOException {

		}
		
		/**
		 * {@inheritDoc}
		 */
		@Override
		public long size() throws IOException {
			return 0;
		}
		
		/**
		 * {@inheritDoc}
		 */
		@Override
		public long getPosition() throws IOException {
			return 0;
		}
		
		/**
		 * {@inheritDoc}
		 */
		@Override
		public void seek(long position) throws IOException {
			if (position != 0) {
				
				throw new EOFException();
			}
		}
	};
	
	/**
	 * Truncates the specified input to the specified size.
	 * 
	 * @param input the input to truncate
	 * @param size the size of the new input
	 * @return a truncated input
	 * @throws IOException if an I/O problem occurs
	 */
	public static SeekableFileDataInput truncate(SeekableFileDataInput input, long size) throws IOException {
		
		return new TruncatedSeekableFileDataInput(input, size);
	}
	
	/**
	 * Truncates the specified input to the specified size.
	 * 
	 * @param input the input to truncate
	 * @param offset the offset
	 * @param size the size of the new input
	 * @return a truncated input
	 * @throws IOException if an I/O problem occurs
	 */
	public static SeekableFileDataInput truncate(SeekableFileDataInput input, long offset, long length) throws IOException {
		
		return new TruncatedSeekableFileDataInput(input, offset, length);
	}
	
	/**
	 * Returns an empty input.
	 * 
	 * @return an empty input
	 */
	public static SeekableFileDataInput empty() {
		
		return EMPTY_INPUT;
	}
	
	/**
	 * Converts the specified <code>ReadableBuffer</code> into a <code>SeekableFileDataInput</code>.
	 * 
	 * @param buffer the readable buffer to convert
	 * @return a <code>SeekableFileDataInput</code> backed by the readable buffer.
	 */
	public static SeekableFileDataInput toSeekableFileDataInput(ReadableBuffer buffer) {
		
		return new SeekableFileDataInputAdapter(buffer);
	}
	
	/**
     * Must not be instantiated.
     */
    private SeekableFileDataInputs() {
    }
}

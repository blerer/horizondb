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

import io.horizondb.io.ReadableBuffer;
import io.horizondb.io.buffers.CompositeBuffer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * <code>SeekableFileDataInput</code> composite.
 * 
 * @author Benjamin
 *
 */
public final class CompositeSeekableFileDataInput extends AbstractFileDataInput implements SeekableFileDataInput {

	/**
	 * The inputs composing this composite.
	 */
	private List<SeekableFileDataInput> inputs = new ArrayList<>();
	
	/**
	 * The index of the current input.
	 */
	private int index;
	
	/**
	 * The offset in bytes of the current input.
	 */
	private long offset;
	
	/**
	 * The total size of all the inputs.
	 */
	private long size;
	
	/**
	 * Adds the specified input to this composite.
	 * 
	 * @param input the input to add.
	 * @return this composite
	 * @throws IOException if an I/O problem occurs
	 */
	public CompositeSeekableFileDataInput add(SeekableFileDataInput input) throws IOException {
		
		input.seek(0);
		
		long inputSize = input.size();
		
		if (inputSize != 0) {
			
			this.inputs.add(input);
			this.size += inputSize;
		}
		
		return this;
	}

	/**
	 * {@inheritDoc}
	 */
    @Override
    public SeekableFileDataInput skipBytes(int numberOfBytes) throws IOException {

    	int remaining = numberOfBytes;
    	
    	while (remaining > 0) {
    	
    		SeekableFileDataInput current = getCurrent();
    	
    		long readableBytes = current.readableBytes();
    			
    		int toSkip = (int) Math.min(remaining, readableBytes);
    	
    		current.skipBytes(toSkip);
    		remaining -= toSkip;
    		
    		if (!current.isReadable()) {
    			
    			next();
    		}
    	}	
    	
	    return this;
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    public byte readByte() throws IOException {
    	
	    SeekableFileDataInput current = getCurrent();
	    
	    if (current.isReadable()) {
	    	return current.readByte();
	    }
	    
	    next();
	    
	    return getCurrent().readByte();
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    public SeekableFileDataInput readBytes(byte[] bytes) throws IOException {

	    return readBytes(bytes, 0, bytes.length);
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    public SeekableFileDataInput readBytes(byte[] bytes, int offset, int length) throws IOException {
    	
    	int remaining = length;
    	int off = offset;
    	
    	while (remaining > 0) {
    	
    		SeekableFileDataInput current = getCurrent();
    	
    		long readableBytes = current.readableBytes();
    			
    		int toCopy = (int) Math.min(remaining, readableBytes);
    	
    		current.readBytes(bytes, off, toCopy);
    		remaining -= toCopy;
    		off += toCopy;
    		
    		if (!current.isReadable()) {
    			
    			next();
    		}
    	}	
    	
	    return this;
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    public ReadableBuffer slice(int length) throws IOException {

    	SeekableFileDataInput current = getCurrent();
    	
		if (current.readableBytes() > length) {
    		
    		return current.slice(length);
    	}
    	
		CompositeBuffer buffer = new CompositeBuffer();
		int remaining = length;
    	
    	while (remaining > 0) {
    	
    		current = getCurrent();
    	
    		long readableBytes = current.readableBytes();
    			
    		int toCopy = (int) Math.min(remaining, readableBytes);
    	
    		buffer.add(current.slice(toCopy));
    		remaining -= toCopy;
    		
    		if (!current.isReadable()) {
    			
    			next();
    		}
    	}	
		
	    return buffer;
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    public boolean isReadable() throws IOException {

	    return readableBytes() > 0;
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    public long getPosition() throws IOException {
    	
    	if (this.index == this.inputs.size()) {
    		
    		return this.offset;
    	}
    	
	    return this.offset + getCurrent().getPosition();
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    public long size() throws IOException {
	    return this.size;
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    public void close() throws IOException {
    	
    	for (int i = 0, m = this.inputs.size(); i < m; i++) {
    		this.inputs.get(i).close();
    	}
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    public void seek(long position) throws IOException {
	    
    	this.index = 0;
    	this.offset = 0;
    	
    	for (int i = 0, m = this.inputs.size(); i < m; i++) {
    		
    		SeekableFileDataInput input = this.inputs.get(i);
    		
    		
    		if (position > this.offset + input.readableBytes()) {
    			
    			next();
    			
    		} else if (position > this.offset) {
    			
    			input.seek(position - this.offset);
    		
    		} else {
    			
    			input.seek(0);
    		}
    	}
    }

	/**
	 * Moves to the next input.
	 * 
	 * @throws IOException if an I/O problem occurs
	 */
    private void next() throws IOException {
    	
	    this.offset += getCurrent().size();
    	this.index++;
    }
    
	/**
	 * Returns the current input.
	 * @return the current input.
	 */
    private SeekableFileDataInput getCurrent() {
	    return this.inputs.get(this.index);
    }
}

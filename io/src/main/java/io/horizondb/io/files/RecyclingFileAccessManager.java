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

import io.horizondb.io.AbstractByteWriter;

import java.io.IOException;
import java.nio.file.Path;

/**
 * @author Benjamin
 *
 */
final class RecyclingFileAccessManager implements FileAccessManager {

	/**
	 * The decorated manager.	
	 */
	private final FileAccessManager manager;
	
	/**
	 * The recycled output;
	 */
	private SeekableFileDataOutput output;
		
	/**
	 * Creates a new <code>RecyclingFileAccessManager</code> that will recycle the output created by the specified
	 * manager.
	 * 
	 * @param manager the decorated manager.
	 */
    public RecyclingFileAccessManager(FileAccessManager manager) {
	    this.manager = manager;
    }

    /**    
     * {@inheritDoc}
     */
	@Override
    public Path getPath() {
	    return this.manager.getPath();
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    public SeekableFileDataInput newInput() throws IOException {
	    return this.manager.newInput();
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    public SeekableFileDataOutput newOuput() throws IOException {

		if (this.output == null) {

			this.output = this.manager.newOuput();
		}

		return new RecycledFileDataOutput(this.output);
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    public long size() throws IOException {
	    return this.manager.size();
    }

	/**
	 * {@inheritDoc}
	 */
    @Override
    public void close() throws IOException {
	    
    	if (this.output != null) {
    	 	this.output.close();
    	}
	    this.manager.close();
    }
    
	/**
     * Decorator that make sure that when the <code>close</code> method is called the output can be reused by another
     * user.
     */
    private final class RecycledFileDataOutput extends AbstractByteWriter implements SeekableFileDataOutput {

		/**
    	 * The decorated output.
    	 */
		private final SeekableFileDataOutput managedOutput;
		
		/**
		 * <code>true</code> if this output is open.
		 */
		private boolean open = true;
    	
		/**
		 * Creates a new <code>RecycledFileDataOutput</code> instance. 
		 * 
		 * @param output the decorated output.
		 */
		public RecycledFileDataOutput(SeekableFileDataOutput output) {
			
			this.managedOutput = output;
		}
    	
    	@Override
        public void seek(long position) throws IOException {
    		
    		checkOpen();
    		this.managedOutput.seek(position);
        }
		
		/**
		 * {@inheritDoc}
		 */
		@Override
    	public SeekableFileDataOutput writeByte(int b) throws IOException {
	        
    		checkOpen();
    		this.managedOutput.writeByte(b);
    		
    		return this;
        }

		/**
		 * {@inheritDoc}
		 */
		@Override
		public long getPosition() throws IOException {
			
			checkOpen();
	        return this.managedOutput.getPosition();
        }

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void flush() throws IOException {
			
			checkOpen();
	        this.managedOutput.flush();
        }

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void close() throws IOException {
			
			this.open = false; 
        }

		/**
		 * {@inheritDoc}
		 */
		@Override
		public SeekableFileDataOutput writeBytes(byte[] bytes, int offset, int length) throws IOException {
			
			checkOpen();
	        this.managedOutput.writeBytes(bytes, offset, length);
	        
	        return this;
        }

		/**
		 * {@inheritDoc}
		 */
		@Override
		public SeekableFileDataOutput writeZeroBytes(int length) throws IOException {
			
			checkOpen();	        
			this.managedOutput.writeZeroBytes(length);
			
			return this;
        }
		
		/**
		 * Checks that this output is still open.
		 * 
		 * @throws IOException if an I/O exception occurs.
		 */
        private void checkOpen() throws IOException {

        	if (!this.open) {
        		throw new IOException("this output is closed");
        	}
        }
    }
}

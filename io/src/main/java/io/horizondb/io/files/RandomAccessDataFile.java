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

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Semaphore;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import static org.apache.commons.lang.Validate.notNull;

/**
 * A <code>RandomAccessFile</code> that allow a single writer and multiple reader.
 * 
 * @author Benjamin
 *
 */
@ThreadSafe
public final class RandomAccessDataFile implements Closeable {
	
	/**
	 * The semaphore used to control the access to the output file.
	 */
	private final Semaphore available = new Semaphore(1);
	
	/**
	 * The manager controlling the file access.
	 */
	@GuardedBy("available")
	private final FileAccessManager manager;
	
	/**
	 * <code>true</code> if the file is closed, <code>false</code> otherwise.  
	 */
	private volatile boolean closed;
	
	/**
	 * Opens the specified file in read-write mode.
	 * 
	 * @param path the file path.
	 * @param keepOutputOpen <code>true</code> if the output must be kept open.
	 * @return the file.
	 * @throws IOException if a problem occurs while opening the file.
	 */
	public static RandomAccessDataFile open(Path path, boolean keepOutputOpen) throws IOException {
		
		FileAccessManager manager = new DirectFileAccessManager(path);
		
		if (keepOutputOpen) {
			
			manager = new RecyclingFileAccessManager(manager);
		}
		
		return new RandomAccessDataFile(manager);
	}

	/**
	 * Opens the specified file in read-write mode truncating or extending it to the specified size.
	 * 
	 * @param path the file path.
	 * @param keepOutputOpen <code>true</code> if the output must be kept open.
	 * @param size the size of the file.
	 * @return the file.
	 * @throws IOException if a problem occurs while opening the file.
	 */
	public static RandomAccessDataFile open(Path path, boolean keepOutputOpen, long size) throws IOException {
		
		FileUtils.extendsOrTruncate(path, size);

		return open(path, keepOutputOpen);
	}
	
	/**
	 * Memory map the specified file, truncating or extending it to the specified size.
	 * 
	 * @param path the file path.
	 * @return the file.
	 * @throws IOException if a problem occurs while opening the file.
	 */
	public static RandomAccessDataFile mmap(Path path) throws IOException {
		
		return new RandomAccessDataFile(new RecyclingFileAccessManager(new MemoryMappedFileAccessManager(path)));
	}
	
	/**
	 * Memory map the specified file.
	 * 
	 * @param path the file path.
	 * @param size the size of the file.
	 * @return the file.
	 * @throws IOException if a problem occurs while opening the file.
	 */
	public static RandomAccessDataFile mmap(Path path, long size) throws IOException {
		
		FileUtils.extendsOrTruncate(path, size);
		
		return mmap(path);
	}
	
	/**
	 * Creates a new <code>RandomAccessFile</code> that used the specified manager to provide access to the file.
	 * 
	 * @param manager the file access manager
	 * @throws IOException if the file does not exists and cannot be created.
	 */
    private RandomAccessDataFile(FileAccessManager manager) throws IOException {
    	
    	notNull(manager, "the manager parameter must not be null.");
    	
    	this.manager = manager;
    }

	/**
	 * Returns <code>true</code> if the file exists on the disk, <code>false</code> otherwise.	
	 * 
	 * @return <code>true</code> if the file exists on the disk, <code>false</code> otherwise.	
	 * @throws IOException if an I/O problem occurs.
	 */
	public boolean exists() throws IOException {
		checkOpen();
		return Files.exists(getPath());
	}
       
	/**
     * Returns the file size.
     * 
     * @return the file size.
     * @throws IOException if an I/O problem occurs while checking the size.
     */
    public long size() throws IOException {
    	checkOpen();
    	return this.manager.size();
    }
    
    /**
     * Returns an output stream to this file. Only one stream can be used to write to the file. This method
     * will block if an other stream is already in use.
     * 
     * @return an output stream to this file.
     * @throws IOException if an I/O problem occurs.
     */
    public SeekableFileDataOutput getOutput() throws IOException {

    	checkOpen();
    	
    	try {
	        
    		this.available.acquire();
    		
	        return new FileDataOutputAccessControler(this.manager.newOuput());
	        
        } catch (InterruptedException e) {

        	this.available.release();
        	
        	Thread.currentThread().interrupt();
        	throw new IllegalStateException(e);
        
        } catch (Exception e) {

        	this.available.release();
         	throw e;
        }
    }

	/**
	 * Returns a new input to read the file data. 
	 * 
	 * @return a new <code>SeekableFileDataInput</code>.
	 * @throws IOException if a problem occurs while creating the input.
	 */
    public SeekableFileDataInput newInput() throws IOException {
    	
    	checkOpen();
    	return this.manager.newInput();
    }
    
	/**
	 * {@inheritDoc}
	 */
    @Override
    public void close() throws IOException {

    	try {
	        
    		this.available.acquire();

    		if (!this.closed) {
    			this.closed = true;
    			this.manager.close();
    		}
    		
        } catch (InterruptedException e) {

        	Thread.currentThread().interrupt();
        	throw new IllegalStateException(e);
        
        } finally {

        	this.available.release();
        }
	}
    
    /**
     * Close this node manager without throwing an exception.
     */
    public void closeQuietly() {
    	
    	try {
    		
    		close();
    		
    	} catch (Exception e) {
    		
    		// Do nothing.
    	}
    }
    
    /**
     * Returns the file path.    
     * @return the file path.
     */
    public Path getPath(){
		return this.manager.getPath();
	}
    
    /**
     * Checks that this file is open.
     * 
     * @throws IOException if the file has been closed.
	 */
    private void checkOpen() throws IOException {
	   
    	if (this.closed) {
    		
    		throw new IOException("The file: " + this.manager.getPath() + " is closed.");
    	}
    }
    
	/**
     * Decorator that make sure that when the <code>close</code> method is called the output is released.
     * 
     */
    private final class FileDataOutputAccessControler extends AbstractByteWriter implements SeekableFileDataOutput {

		/**
    	 * The decorated output.
    	 */
		private final SeekableFileDataOutput managedOutput;
		
		/**
		 * Creates a new <code>FileDataOutputManager</code> instance. 
		 * 
		 * @param output the decorated output.
		 */
		public FileDataOutputAccessControler(SeekableFileDataOutput output) {
			
			this.managedOutput = output;
		}
		
    	/**
    	 * 
    	 * {@inheritDoc}
    	 */
    	@Override
        public void seek(long position) throws IOException {
    		
    		this.managedOutput.seek(position);
        }
		
		/**
		 * {@inheritDoc}
		 */
		@Override
    	public SeekableFileDataOutput writeByte(int b) throws IOException {
	        
    		this.managedOutput.writeByte(b);
    		
    		return this;
        }

		/**
		 * {@inheritDoc}
		 */
		@Override
		public long getPosition() throws IOException {
			
	        return this.managedOutput.getPosition();
        }

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void flush() throws IOException {
			
	        this.managedOutput.flush();
        }

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void close() throws IOException {
			
			this.managedOutput.close();

			RandomAccessDataFile.this.available.release();
        }

		/**
		 * {@inheritDoc}
		 */
		@Override
		public SeekableFileDataOutput writeBytes(byte[] bytes, int offset, int length) throws IOException {
			
	        this.managedOutput.writeBytes(bytes, offset, length);
	        
	        return this;
        }

		/**
		 * {@inheritDoc}
		 */
		@Override
		public SeekableFileDataOutput writeZeroBytes(int length) throws IOException {
			       
			this.managedOutput.writeZeroBytes(length);
			
			return this;
        }
    }
}

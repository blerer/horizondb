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

import io.netty.util.internal.PlatformDependent;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * Utility methods to work with files.
 *   
 * @author benjamin
 *
 */
public final class FileUtils {
    
    /**
     * The number of bytes in a kilobyte.
     */
    public static final int ONE_KB = 1024;

    /**
     * The number of bytes in a megabyte.
     */
    public static final int ONE_MB = ONE_KB * ONE_KB;

    /**
     * The number of bytes in a gigabyte.
     */
    public static final int ONE_GB = ONE_KB * ONE_MB;
    
    /**
     * <code>FileVisitor</code> used to delete directory content.
     */
    private static SimpleFileVisitor<Path> DELETER = new SimpleFileVisitor<Path>() {

        /**
         * {@inheritDoc}
         */
        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {

            file.toFile().delete();
            
            return FileVisitResult.CONTINUE;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {

            dir.toFile().delete();
            
            return FileVisitResult.CONTINUE;
        }
    };
    
    /**
     * Opens the specified random access file.
     *   
     * @param path the path to the file
     * @return the random access file
     * @throws FileNotFoundException if the file does not exists.
     */
    public static RandomAccessFile openRandomAccessFile(Path path) throws FileNotFoundException {
        
        return new RandomAccessFile(path.toFile(), "rw");
    }
    
    /**
     * Extends or truncates to the specified length the specified file.
     * 
     * @param path the path to the file to extends or truncate
     * @param length the expected length of the file after the extension or the truncation
     * @throws IOException if a problem occurs during the operation.
     */
    public static void extendsOrTruncate(Path path, long length) throws IOException {
        
    	if (!Files.exists(path) && length == 0) {
    		return;
    	}
    	
		try (RandomAccessFile file = openRandomAccessFile(path)) {
			
			extendsOrTruncate(file, length);
		}
    }
    
    /**
     * Extends or truncates to the specified length the specified file.
     * 
     * @param file the file to extends or truncate
     * @param length the expected length of the file after the extension or the truncation
     * @throws IOException if a problem occurs during the operation.
     */
    public static void extendsOrTruncate(RandomAccessFile file, long length) throws IOException {
        
        file.setLength(length);
    }
    
    /**
     * Memory map the specified portion of the specified file. 
     * 
     * @param path the file path.
     * @param  position the position within the file at which the mapped region
     *         is to start; must be non-negative
     * @param  size the size of the region to be mapped; must be non-negative and
     *         no greater than {@link j
     *         ava.lang.Integer#MAX_VALUE}
     * @return the mapped byte buffer
     * @throws IOException if a problem occurs while mapping the file.
     */
     public static MappedByteBuffer mmap(Path path, long position, long size) throws IOException {
        
        try (FileChannel channel = openChannel(path)) {
                                    
            return mmap(channel, position, size);        
        } 
    }

     /**
      * Memory map the specified  file. 
      * 
      * @param path the file path.
      * @return the mapped byte buffer
      * @throws IOException if a problem occurs while mapping the file.
      */
      public static MappedByteBuffer mmap(Path path) throws IOException {
         
         try (FileChannel channel = openChannel(path)) {
                                     
             return mmap(channel, 0, channel.size());        
         } 
     }
     
     /**
      * Memory map the specified portion of the file corresponding to the specified channel. 
      * 
      * @param channel the file channel.
      * @param position the position within the file at which the mapped region is to start; must be non-negative
      * @param size the size of the region to be mapped; must be non-negative and 
      * no greater than {@link java.lang.Integer#MAX_VALUE}
      * @return the mapped byte buffer
      * @throws IOException if a problem occurs while mapping the file.
      */
     public static MappedByteBuffer mmap(FileChannel channel, long position, long size) throws IOException {
        
        return channel.map(MapMode.READ_WRITE, position, size);
    }
     
     /**
      * Memory map the specified portion of the specified file. 
      * 
      * @param file the random access file to memory map.
      * @param position the position within the file at which the mapped region is to start; must be non-negative
      * @param size the size of the region to be mapped; must be non-negative and 
      * no greater than {@link java.lang.Integer#MAX_VALUE}
      * @return the mapped byte buffer
      * @throws IOException if a problem occurs while mapping the file.
      */
     public static MappedByteBuffer mmap(RandomAccessFile file, long position, long size) throws IOException {
        
        return mmap(file.getChannel(), position, size);
    }
    
    /**
     * Unmap the specified memory mapping.
     * 
     * @param buffer the buffer representing the memory mapping.
     */
    public static void munmap(MappedByteBuffer buffer) {
        PlatformDependent.freeDirectBuffer(buffer);
    }
    
    /**
     * Forces the deletion of the file or directory corresponding to the specified path.
     * 
     * @param path the path of the file or directory to delete.
     * @throws IOException if a problem occurs while deleting the file or directory.
     */
    public static void forceDelete(Path path) throws IOException {
        Files.walkFileTree(path, DELETER);
    }
    
    /**
     * Creates a file with the specified content.
     *
     * @param filePath the file path.
     * @param content  the file content.
     * @throws java.io.IOException if the file cannot be created.
     */
    public static void createFile(Path filePath, byte[] content) throws IOException {
        
        try (OutputStream output = Files.newOutputStream(filePath)) {
            output.write(content);
            output.flush();
        }
    }
        
    /**
     * Opens a file channel to the specified path.
     * 
     * @param path the file path.
     * @return a file channel to the specified path.
     * @throws FileNotFoundException if the file cannot be found.
     */
    private static FileChannel openChannel(Path path) throws IOException {

        return (FileChannel) Files.newByteChannel(path, 
                                                  StandardOpenOption.CREATE, 
                                                  StandardOpenOption.READ, 
                                                  StandardOpenOption.WRITE);
    }
    
    /**
     * Must not be instantiated.
     */
    private FileUtils() {

    }
}

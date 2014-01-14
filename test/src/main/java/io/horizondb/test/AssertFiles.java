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
package io.horizondb.test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Asserts utilities for files.
 * 
 * @author Benjamin
 *
 */
public final class AssertFiles {

    /**
     * Verify that the specified file contains only the specified bytes. 
     * @param expected the expected content
     * @param path the file path 
     */
    public static void assertFileContains(byte[] expected, Path path) throws IOException {
        
        assertFileExists(path);
        assertFileSize(expected.length, path);
        assertArrayEquals("The content of the file does not match the expected one.", expected, getFileContent(path));
    }

    /**
     * Verify that the specified file contains only the specified bytes starting at the specified position. 
     * 
     * @param position the position of the expected bytes
     * @param expected the expected content
     * @param path the file path 
     */
    public static void assertFileContainsAt(int position, byte[] expected, Path path) throws IOException {
        
        assertFileExists(path);

        byte[] bytes = new byte[expected.length];
        System.arraycopy(getFileContent(path), position, bytes, 0, bytes.length);
        
        assertArrayEquals("The content of the file at the position " + position + " does not match the expected one.", 
                          expected, 
                          bytes);
    }
    
    /**
     * Verify that the specified file is a directory. 
     * 
     * @param path the directory path 
     */
    public static void assertDirectory(Path path) {

        assertTrue("The file " + path + " is not a directory.", Files.isDirectory(path));
    }
    
    /**
     * Verify that the specified file is a directory and is empty. 
     * 
     * @param path the directory path 
     */
    public static void assertDirectoryEmpty(Path path) {

        assertDirectory(path);
        
        File[] files = path.toFile().listFiles();
        
        assertEquals("The directory " + path + " contains an unexpected number of files.", 0, files.length);
    }
    
    /**
     * Verify that the specified file exists. 
     * 
     * @param path the file path 
     */
    public static void assertFileExists(Path path) {

        assertTrue("The file " + path + " does not exists.", Files.exists(path));
    }
    
    /**
     * Verify that the specified file does not exists. 
     * 
     * @param path the file path 
     */
    public static void assertFileDoesNotExists(Path path) {

        assertFalse("The file " + path + " exists.", Files.exists(path));
    }
    
    /**
     * Verify that the specified file has the specified size. 
     * 
     * @param expected the expected size
     * @param path the file path 
     */
    public static void assertFileSize(long expected, Path path) throws IOException {

        assertEquals("The file size does not match the expected one.", expected, Files.size(path));
    }
    
    /**
     * Returns the content of the specified file as a byte array.
     * 
     * @param path the file path.
     * @return the content of the specified file as a byte array.
     * @throws IOException if a problem occurs while accessing the file.
     */
    private static byte[] getFileContent(Path path) throws IOException {

        int size = (int) Files.size(path);
        
        byte[] content = new byte[size];
        
        try (InputStream input = Files.newInputStream(path)) {
        
            input.read(content);
            return content;   
        }
    }
 
    private AssertFiles() {
        
    }
}

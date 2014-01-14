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

import io.horizondb.io.files.FileUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;


import org.junit.Assert;
import org.junit.Test;

public class FileUtilsTest {

    @SuppressWarnings("unused")
    @Test
    public void testForceDeleteWithDirectory() throws IOException {
        
        Path testDirectory = Files.createTempDirectory("test");
        Path dirA = Files.createDirectory(testDirectory.resolve("A"));
        Path file1 = Files.createFile(dirA.resolve("1.txt"));
        Path dirB = Files.createDirectory(dirA.resolve("B"));
        Path dirC = Files.createDirectory(dirA.resolve("C"));
        Path file2 = Files.createFile(dirC.resolve("2.txt"));
        
        FileUtils.forceDelete(dirA);
        Assert.assertFalse(Files.exists(dirA));
        
        Files.delete(testDirectory);
    }
    
    @Test
    public void testForceDeleteWithFile() throws IOException {
        
        Path testDirectory = Files.createTempDirectory("test");
        Path file1 = Files.createFile(testDirectory.resolve("1.txt"));
        
        FileUtils.forceDelete(file1);
        Assert.assertFalse(Files.exists(file1));
        
        Files.delete(testDirectory);
    }
}

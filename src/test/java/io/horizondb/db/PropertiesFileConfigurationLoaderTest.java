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
package io.horizondb.db;

import java.io.IOException;

import org.junit.Test;

/**
 * @author Benjamin
 * 
 */
public class PropertiesFileConfigurationLoaderTest {
    
    @Test
    public void testLoadConfigurationFromClassPath() throws IOException, HorizonDBException {

        PropertiesFileConfigurationLoader loader = new PropertiesFileConfigurationLoader();

        Configuration configuration = loader.loadConfigurationFromClasspath("horizondb-config.properties");
    }
}

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

import io.horizondb.db.Configuration.Builder;
import io.horizondb.model.ErrorCodes;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.PropertyResourceBundle;

/**
 * <code>ConfigurationLoader</code> that loads the configuration from a properties file.
 * 
 * @author Benjamin
 * 
 */
final class PropertiesFileConfigurationLoader implements ConfigurationLoader {

    /**
     * The methods of the <code>Configuration.Builder</code>.
     */
    private final Map<String, Method> methods = new HashMap<>();

    /**
     * Creates a new <code>PropertiesFileConfigurationLoader</code>
     */
    public PropertiesFileConfigurationLoader() {

        Method[] declaredMethods = Builder.class.getDeclaredMethods();

        for (Method method : declaredMethods) {

            this.methods.put(method.getName(), method);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Configuration loadConfigurationFrom(InputStream input) throws IOException, HorizonDBException {

        PropertyResourceBundle resourceBundle = new PropertyResourceBundle(input);

        Builder builder = Configuration.newBuilder();

        for (String key : resourceBundle.keySet()) {

            setProperty(builder, key, resourceBundle.getString(key));
        }

        return builder.build();
    }

    /**
     * Sets the specified property on the specified builder.
     * 
     * @param builder the configuration builder
     * @param propertyName the property name
     * @param value the property value
     * @throws HorizonDBException if the property cannot be converted
     */
    private void setProperty(Builder builder, String propertyName, String value) throws HorizonDBException {

        Method method = this.methods.get(propertyName);

        try {

            method.invoke(builder, toType(propertyName, value.trim(), method.getParameterTypes()[0]));

        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {

            throw new HorizonDBException(ErrorCodes.INVALID_CONFIGURATION, "The configuration property: "
                    + propertyName + " does not exists.");
        }
    }

    /**
     * Converts the specified <code>String</code> value into the specified type.
     * 
     * @param propertyName the property name
     * @param value the value to convert
     * @param type the type into which the value must be converted
     * @return the converted value
     * @throws HorizonDBException if the value cannot be converted
     */
    @SuppressWarnings("boxing")
    private static Object toType(String propertyName, String value, Class<?> type) throws HorizonDBException {

        try {

            if (int.class.equals(type)) {

                return Integer.parseInt(value);
            }

            if (long.class.equals(type)) {

                return Long.parseLong(value);
            }

            if (Path.class.equals(type)) {

                return FileSystems.getDefault().getPath(value);
            }

        } catch (NumberFormatException e) {

            throw new HorizonDBException(ErrorCodes.INVALID_CONFIGURATION, "The value: '" + value
                    + "' associated to the property: " + propertyName + " is not a valid " + type + ".");
        }

        throw new IllegalStateException("Convertion to the type: " + type + " is not supported.");
    }
}

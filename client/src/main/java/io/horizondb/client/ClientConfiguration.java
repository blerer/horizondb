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
package io.horizondb.client;

import java.net.InetSocketAddress;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * Client configuration.
 *
 */
final class ClientConfiguration {
    
    /**
     * The default timeout of 30 seconds.
     */
    private static final int DEFAULT_TIMEOUT = 30;

    /**
     * The address of the server.
     */
    private final InetSocketAddress hostAddress;
    
    /**
     * The query timeout in second.
     */
    private int queryTimeoutInSeconds = DEFAULT_TIMEOUT;
    
    /**
     * Creates a new <code>ClientConfiguration</code> instance.
     * 
     * @param name the name of this client.
     * @param hostAddress The address of the server.
     */
    public ClientConfiguration(InetSocketAddress hostAddress) {
        
    	Validate.notNull(hostAddress, "The hostAddress parameter must not be null.");
    	this.hostAddress = hostAddress;
    }

    /**    
     * Returns the address of the server.
     * @return The address of the server.
     */
    public InetSocketAddress getHostAddress() {
        return this.hostAddress;
    }

	/**
	 * Returns the query timeout in seconds.
	 * 
	 * @return the query timeout in seconds.
	 */
	public int getQueryTimeoutInSeconds() {
		return this.queryTimeoutInSeconds;
	}
	
	/**
	 * Sets the query timeout in seconds.
	 * 
	 * @param queryTimeout the new query timeout in seconds.
	 */
    public void setQueryTimeoutInSeconds(int queryTimeoutInSeconds) {
	    this.queryTimeoutInSeconds = queryTimeoutInSeconds;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object object) {
        if (object == this) {
            return true;
        }
        if (!(object instanceof ClientConfiguration)) {
            return false;
        }
        ClientConfiguration rhs = (ClientConfiguration) object;
        
        return new EqualsBuilder().append(this.hostAddress, rhs.hostAddress)
                                  .append(this.queryTimeoutInSeconds, rhs.queryTimeoutInSeconds)
                                  .isEquals();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return new HashCodeBuilder(-663727339, 695305331).append(this.hostAddress)
                                                         .append(this.queryTimeoutInSeconds)
                                                         .toHashCode();
    }/**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("hostAddress", this.hostAddress)
                                                                          .append("queryTimeoutInSeconds", 
                                                                                  this.queryTimeoutInSeconds)
                                                                          .toString();
    }
}

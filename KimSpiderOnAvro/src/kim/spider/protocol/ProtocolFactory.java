/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kim.spider.protocol;

import java.net.URL;

import kim.spider.protocol.httpclient.HttpSimply;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * Creates and caches {@link Protocol} plugins. Protocol plugins should define
 * the attribute "protocolName" with the name of the protocol that they
 * implement. Configuration object is used for caching. Cache key is constructed
 * from appending protocol name (eg. http) to constant
 * {@link Protocol#X_POINT_ID}.
 */
public class ProtocolFactory {

	public static final Log LOG = LogFactory.getLog(ProtocolFactory.class);

	
	private Configuration conf;

	public ProtocolFactory(Configuration conf) {
		this.conf = conf;
	}

	/**
	 * Returns the appropriate {@link Protocol} implementation for a url.
	 * 
	 * @param urlString
	 *            Url String
	 * @return The appropriate {@link Protocol} implementation for a given
	 *         {@link URL}.
	 * @throws ProtocolNotFound
	 *             when Protocol can not be found for urlString
	 */
	public Protocol getProtocol(String urlString) throws ProtocolNotFound {
		
		Protocol po = new HttpSimply();
		po.setConf(conf);
		return  po;
//		try {
//			URL url = new URL(urlString);
//			String protocolName = url.getProtocol();
//		} catch (MalformedURLException e) {
//			throw new ProtocolNotFound(urlString, e.toString());
//		}
	}

}

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

package kim.spider.parse;

import java.net.MalformedURLException;

import kim.spider.net.BasicURLNormalizer;

import org.apache.avro.util.Utf8;

/* An outgoing link from a page. */
public class Outlink extends kim.spider.schema.Outlink {

	// 赋予每个外链一个周期,默认1000分钟
	public Outlink() {
	}

	public Outlink(String toUrl, String anchor) throws MalformedURLException {
		if (toUrl == null)
			toUrl = "";
		this.url = new BasicURLNormalizer().normalize(toUrl);
		if (anchor == null)
			anchor = "";
		this.anchor = anchor;
	}
	
	 public String getUrl() {
			return url.toString();
		}
		public void setUrl(String url) {
			this.url = new Utf8(url);
		}
		public String getAnchor() {
			return anchor.toString();
		}
		public void setAnchor(String anchor) {
			this.anchor = new Utf8(anchor);
		}
		public int getFetchInterval() {
			return fetchInterval;
		}
		public void setFetchInterval(int fetchInterval) {
			this.fetchInterval = fetchInterval;
		}
		public java.util.Map<java.lang.CharSequence, java.lang.CharSequence> getExtend() {
			if(extend == null)
				this.extend = new java.util.HashMap<java.lang.CharSequence, java.lang.CharSequence>();
			return extend;
		}
		public void setExtend(
				java.util.Map<java.lang.CharSequence, java.lang.CharSequence> extend) {
			this.extend = extend;
		}
		
		public void addExtend(String name,String value)
		{
			this.getExtend().put(new Utf8(name), new Utf8(value));
		}
}

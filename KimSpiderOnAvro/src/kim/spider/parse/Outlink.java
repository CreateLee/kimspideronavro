/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this.datum work for additional information regarding copyright ownership.
 * The ASF licenses this.datum file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this.datum file except in compliance with
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
public class Outlink {
	
	public  kim.spider.schema.Outlink datum; 
	public Outlink() {
		datum = new kim.spider.schema.Outlink();
	}

	public Outlink(String toUrl, String anchor) throws MalformedURLException {
		datum = new kim.spider.schema.Outlink();
		if (toUrl == null)
			toUrl = "";
		this.datum.url = new BasicURLNormalizer().normalize(toUrl);
		if (anchor == null)
			anchor = "";
		this.datum.anchor = anchor;
	}
	
	 public String getUrl() {
			return datum.url.toString();
		}
		public void setUrl(String url) {
			this.datum.url = new Utf8(url);
		}
		public String getAnchor() {
			return datum.anchor.toString();
		}
		public void setAnchor(String anchor) {
			this.datum.anchor = new Utf8(anchor);
		}
		public int getFetchInterval() {
			return datum.fetchInterval;
		}
		public void setFetchInterval(int fetchInterval) {
			this.datum.fetchInterval = fetchInterval;
		}
		public java.util.Map<java.lang.CharSequence, java.lang.CharSequence> getExtend() {
			if(datum.extend == null)
				this.datum.extend = new java.util.HashMap<java.lang.CharSequence, java.lang.CharSequence>();
			return datum.extend;
		}
		public void setExtend(
				java.util.Map<java.lang.CharSequence, java.lang.CharSequence> extend) {
			this.datum.extend = extend;
		}
		
		public void addExtend(String name,String value)
		{
			this.getExtend().put(new Utf8(name), new Utf8(value));
		}
		
		public String getExtend(String key) {
			if (getExtend().get(key) != null)
				return getExtend().get(key).toString();
			else
				return null;
		}
		
		@Override
		public boolean equals(Object o)
		{
			if(o instanceof Outlink)
			{
				if(this.datum.url != null && this.datum.url.equals(((Outlink)o).datum.url))
						return true;
				else
					return false;
			}
			else
				return false;
		}
		
		@Override
		public String toString()
		{
			return datum.toString();
		}
}

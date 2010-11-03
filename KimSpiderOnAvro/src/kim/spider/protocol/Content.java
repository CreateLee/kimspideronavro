/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * datum work for additional information regarding copyright ownership.
 * The ASF licenses datum file to You under the Apache License, Version 2.0
 * (the "License"); you may not use datum file except in compliance with
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

//JDK imports
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class Content {

	public static final String	DIR_NAME	= "content";

	public kim.spider.schema.Content		datum;

	public Content() {
	}

	public Content(String url, byte[] content,Map<java.lang.CharSequence, java.util.List<java.lang.CharSequence>> metaData) {

		if (url == null)
			throw new IllegalArgumentException("null url");
		if (content == null)
			throw new IllegalArgumentException("null content");
		datum = new kim.spider.schema.Content();
		datum.url = url;
		if (content != null) {
			if (datum.content == null)
				datum.content = java.nio.ByteBuffer.allocate(content.length);
			datum.content.put(content);
			datum.content.rewind();
			this.setMetadata(metaData);
		}
	}
	
	public Content(kim.spider.schema.Content content)
	{
		this.datum = content;
	}
	/** The url fetched. */
	public String getUrl() {
		return datum.url.toString();
	}

	/** The binary content retrieved. */
	public byte[] getContent() {
		if (datum.content != null)
			return datum.content.array();
		else
			return null;
	}

	public void setContent(byte[] content) {
		datum.content.put(content);
		datum.content.rewind();
	}

	public java.util.Map<java.lang.CharSequence, java.util.List<java.lang.CharSequence>> getMetadata() {
		if (datum.metaData == null)
			datum.metaData = new java.util.HashMap<java.lang.CharSequence, java.util.List<java.lang.CharSequence>>();
		return datum.metaData;
	}

	
	public void setMetadata(
			java.util.Map<java.lang.CharSequence, java.util.List<java.lang.CharSequence>> metaData) {
		datum.metaData = metaData;
	}
	
		public static void main(String argv[]) throws Exception {

	}

	public void addMetadata(String name, String value) {
		List<java.lang.CharSequence> val = getMetadata().remove(name);
		if (val == null)
			val = new ArrayList<java.lang.CharSequence>();
		val.add(value);
		getMetadata().put(name, val);
	}

	public void addMetadata(String name, String[] value) {
		List<java.lang.CharSequence> val = getMetadata().remove(name);
		if (val == null)
			val = new ArrayList<java.lang.CharSequence>();
		for (String s : value) {
			val.add(s);
		}
		getMetadata().put(name, val);
	}

	public String getMeta(String name) {
		List<java.lang.CharSequence> val = getMetadata().get(name);
		if (val == null || val.size() < 1)
			return null;
		else
			return val.get(0).toString();
	}
	
	public java.util.Map<java.lang.CharSequence,java.lang.CharSequence> getExtendData() {
		if (datum.extend == null)
			datum.extend = new java.util.HashMap<java.lang.CharSequence,java.lang.CharSequence>();
		return datum.extend;
	}
	
	public void setExtendData(java.util.Map<java.lang.CharSequence,java.lang.CharSequence> extend) {
		datum.extend = extend;
	}
	
	public void setExtend(String key,String value) {
		getExtendData().put(key, value);
	}
	
	public String getExtend(String key) {
		return getExtendData().get(key).toString();
	}
	
}
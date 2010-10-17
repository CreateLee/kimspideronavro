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

package kim.spider.crawl;

import java.util.HashMap;
import java.util.Map.Entry;

import kim.spider.schema.BYTE;

/* The crawl state of a url. */
public class CrawlDatum implements  Cloneable {
	public static final String GENERATE_DIR_NAME = "crawl_generate";
	public static final String FETCH_DIR_NAME = "crawl_fetch";
	public static final String PARSE_DIR_NAME = "crawl_parse";

	/** Page was not fetched yet. */
	public static final byte STATUS_DB_UNFETCHED = 0x01;
	/** Page was successfully fetched. */
	public static final byte STATUS_DB_FETCHED = 0x02;
	/** Page no longer exists. */
	public static final byte STATUS_DB_GONE = 0x03;
	/** Page temporarily redirects to other page. */
	public static final byte STATUS_DB_REDIR_TEMP = 0x04;
	/** Page permanently redirects to other page. */
	public static final byte STATUS_DB_REDIR_PERM = 0x05;
	/** Page was successfully fetched and found not modified. */
	public static final byte STATUS_DB_NOTMODIFIED = 0x06;

	/** Maximum value of DB-related status. */
	public static final byte STATUS_DB_MAX = 0x1f;

	/** Fetching was successful. */
	public static final byte STATUS_FETCH_SUCCESS = 0x21;
	/** Fetching unsuccessful, needs to be retried (transient errors). */
	public static final byte STATUS_FETCH_RETRY = 0x22;
	/** Fetching temporarily redirected to other page. */
	public static final byte STATUS_FETCH_REDIR_TEMP = 0x23;
	/** Fetching permanently redirected to other page. */
	public static final byte STATUS_FETCH_REDIR_PERM = 0x24;
	/** Fetching unsuccessful - page is gone. */
	public static final byte STATUS_FETCH_GONE = 0x25;
	/** Fetching successful - page is not modified. */
	public static final byte STATUS_FETCH_NOTMODIFIED = 0x26;

	/** Maximum value of fetch-related status. */
	public static final byte STATUS_FETCH_MAX = 0x3f;

	/** Page signature. */
	public static final byte STATUS_SIGNATURE = 0x41;
	/** Page was newly injected. */
	public static final byte STATUS_INJECTED = 0x42;
	/** Page discovered through a link. */
	public static final byte STATUS_LINKED = 0x43;
	/** Page got metadata from a parser */
	public static final byte STATUS_PARSE_META = 0x44;

	public static final HashMap<Byte, String> statNames = new HashMap<Byte, String>();
	static {
		statNames.put(STATUS_DB_UNFETCHED, "db_unfetched");
		statNames.put(STATUS_DB_FETCHED, "db_fetched");
		statNames.put(STATUS_DB_GONE, "db_gone");
		statNames.put(STATUS_DB_REDIR_TEMP, "db_redir_temp");
		statNames.put(STATUS_DB_REDIR_PERM, "db_redir_perm");
		statNames.put(STATUS_DB_NOTMODIFIED, "db_notmodified");
		statNames.put(STATUS_SIGNATURE, "signature");
		statNames.put(STATUS_INJECTED, "injected");
		statNames.put(STATUS_LINKED, "linked");
		statNames.put(STATUS_FETCH_SUCCESS, "fetch_success");
		statNames.put(STATUS_FETCH_RETRY, "fetch_retry");
		statNames.put(STATUS_FETCH_REDIR_TEMP, "fetch_redir_temp");
		statNames.put(STATUS_FETCH_REDIR_PERM, "fetch_redir_perm");
		statNames.put(STATUS_FETCH_GONE, "fetch_gone");
		statNames.put(STATUS_FETCH_NOTMODIFIED, "fetch_notmodified");
		statNames.put(STATUS_PARSE_META, "parse_metadata");

	}
	public kim.spider.schema.CrawlDatum datum;
	public static boolean hasDbStatus(CrawlDatum datum) {
		if (datum.getStatus() <= STATUS_DB_MAX)
			return true;
		return false;
	}

	public static boolean hasFetchStatus(CrawlDatum datum) {
		if (datum.getStatus() > STATUS_DB_MAX && datum.getStatus() <= STATUS_FETCH_MAX)
			return true;
		return false;
	}

	public CrawlDatum() {
	}

	public CrawlDatum(int status, int fetchInterval) {
		datum = new kim.spider.schema.CrawlDatum();
		this.setStatus(status);
		datum.fetchInterval = fetchInterval;
	}
	
	public CrawlDatum(kim.spider.schema.CrawlDatum datum) {
		this.datum = datum; 
	}
	
	public CrawlDatum(int status, int fetchInterval, float score) {
		this(status, fetchInterval);
		datum.score = score;
	}

	public byte getStatus() {
		if(datum.status == null)
			datum.status = new BYTE();
		return datum.status.bytes()[0];
	}

	public static String getStatusName(byte value) {
		String res = statNames.get(value);
		if (res == null)
			res = "unknown";
		return res;
	}

	public void setStatus(int status) {
		if(datum.status == null)
			datum.status = new BYTE();
		datum.status.bytes()[0] = (byte) status;
	}

	/**
	 * Returns either the time of the last fetch, or the next fetch time,
	 * depending on whether Fetcher or CrawlDbReducer set the time.
	 */
	public long getFetchTime() {
		return datum.fetchTime;
	}

	/**
	 * Sets either the time of the last fetch or the next fetch time, depending
	 * on whether Fetcher or CrawlDbReducer set the time.
	 */
	public void setFetchTime(long fetchTime) {
		datum.fetchTime = fetchTime;
	}

	public long getModifiedTime() {
		return datum.modifiedTime;
	}

	public void setModifiedTime(long modifiedTime) {
		datum.modifiedTime = modifiedTime;
	}

	public int getRetriesSinceFetch() {
		return datum.retries;
	}

	public void setRetriesSinceFetch(int retries) {
		datum.retries = retries;
	}

	public int getFetchInterval() {
		return datum.fetchInterval;
	}

	public void setFetchInterval(int fetchInterval) {
		datum.fetchInterval = fetchInterval;
	}

	public void setFetchInterval(float fetchInterval) {
		datum.fetchInterval = Math.round(fetchInterval);
	}

	public float getScore() {
		return datum.score;
	}

	public void setScore(float score) {
		datum.score = score;
	}

	public byte[] getSignature() {
		return datum.signature.bytes();
	}

	public void setSignature(byte[] signature) {
		if (signature != null && signature.length > 256)
			throw new RuntimeException("Max signature length (256) exceeded: "
					+ signature.length);
		datum.signature.bytes(signature);
	}

	public void setMetaData(java.util.Map<java.lang.CharSequence,java.lang.CharSequence> meta) {
		datum.metaData = meta;
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
	
	public void setMeta(String key,String value) {
		getMetaData().put(key, value);
	}
	
	/**
	 * Add all metadata from other CrawlDatum to datum CrawlDatum.
	 * 
	 * @param other
	 *            CrawlDatum
	 */
	public void putAllMetaData(CrawlDatum other) {
		for (Entry<java.lang.CharSequence,java.lang.CharSequence> e : other.getMetaData().entrySet()) {
			getMetaData().put(e.getKey(), e.getValue());
		}
	}

		
	public void putAllExtendData(CrawlDatum other) {
		for (Entry<java.lang.CharSequence,java.lang.CharSequence> e : other.getExtendData().entrySet()) {
			getExtendData().put(e.getKey(), e.getValue());
		}
	}
	
	public java.util.Map<java.lang.CharSequence,java.lang.CharSequence> getMetaData() {
		if (datum.metaData == null)
			datum.metaData = new java.util.HashMap<java.lang.CharSequence,java.lang.CharSequence>();
		return datum.metaData;
	}
	
	
	
	/** Copy the contents of another instance into datum instance. */
	public void set(CrawlDatum that) {
		datum = new kim.spider.schema.CrawlDatum();
		datum.status = that.datum.status;
		datum.fetchTime = that.datum.fetchTime;
		datum.retries = that.datum.retries;
		datum.fetchInterval = that.datum.fetchInterval;
		datum.score = that.datum.score;
		datum.modifiedTime = that.datum.modifiedTime;
		datum.signature = that.datum.signature;
		if (that.datum.metaData != null) {
			putAllMetaData(that); // make
		} else {
			datum.metaData = null;
		}
		if(that.datum.extend != null)
		{
			this.putAllExtendData(that);
		}
		else
			datum.extend = null;
	}

	public Object clone() {
		try {
			return super.clone();
		} catch (CloneNotSupportedException e) {
			throw new RuntimeException(e);
		}
	}
}

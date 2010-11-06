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

package kim.spider.crawl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import kim.spider.metadata.Spider;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.PriorityQueue;

/** Merge new page entries with existing entries. */
public class CrawlDbReducer extends Reducer<String, kim.spider.schema.CrawlDatum, String, kim.spider.schema.CrawlDatum> {
	public static final Log			LOG			= LogFactory.getLog(CrawlDbReducer.class);

	private int									retryMax;
	private CrawlDatum					result	= new CrawlDatum();
	private boolean							additionsAllowed;
	private int									maxInterval;
	private FetchSchedule				schedule;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		Configuration job = context.getConfiguration();
		retryMax = job.getInt("db.fetch.retry.max", 3);
		additionsAllowed = job.getBoolean(CrawlDb.CRAWLDB_ADDITIONS_ALLOWED, true);
		int oldMaxInterval = job.getInt("db.max.fetch.interval", 0);
		maxInterval = job.getInt("db.fetch.interval.max", 0);
		if (oldMaxInterval > 0 && maxInterval == 0)
			maxInterval = oldMaxInterval * FetchSchedule.SECONDS_PER_DAY;
		schedule = FetchScheduleFactory.getFetchSchedule(job);
		int maxLinks = job.getInt("db.update.max.inlinks", 10000);
	}
	private ArrayList<CrawlDatum> linked = new ArrayList<CrawlDatum>();
	@Override
	protected void reduce(String key, Iterable<kim.spider.schema.CrawlDatum> values, Context context)
			throws IOException, InterruptedException {
		CrawlDatum fetch = null;
		CrawlDatum old = null;
		CrawlDatum gen = null;
		linked.clear();

		while (values.iterator().hasNext())
		{
			CrawlDatum datum = new CrawlDatum(values.iterator().next());
			if (CrawlDatum.hasDbStatus(datum))
			{
				if (old == null)
				{
					old = new CrawlDatum();
					old.set(datum);
				} else
				{
					// always take the latest version
					if (old.getFetchTime() < datum.getFetchTime())
						old.set(datum);
				}
				continue;
			}

			if (CrawlDatum.hasFetchStatus(datum))
			{
				if (fetch == null)
				{
					fetch = new CrawlDatum();
					fetch.set(datum);
				} else
				{
					if (fetch.getFetchTime() < datum.getFetchTime())
						fetch.set(datum);
				}
				continue;
			}
			if (datum.getStatus() == CrawlDatum.STATUS_LINKED)
				linked.add(datum);
//			if(datum.getStatus() == CrawlDatum.STATUS_GENERATER)
//			{
//				if(gen == null)
//					gen = datum; 
//			}
		}

		if (fetch==null && linked.size() > 0)
		{
			fetch = (CrawlDatum) linked.get(0);
		}
		// still no new data - record only unchanged old data, if exists, and
		// return
		if (fetch == null)
		{
			if (old != null && gen != null) // at this point at least "old" should be
			{
				old.getMetaData().remove(Spider.GENERATE_TIME_KEY);
				context.write(key, old.datum);
			}
			else if(old != null)
			{
				context.write(key, old.datum);
			}
			return;
		}
		
		// initialize with the latest version, be it fetch or link
		result.set(fetch);
		if (old != null)
		{
			// copy metadata from old, if exists
			if (old.getMetaData().size() > 0)
			{
				result.getMetaData().putAll(old.getMetaData());
				// overlay with new, if any
				if (fetch.getMetaData().size() > 0)
					result.getMetaData().putAll(fetch.getMetaData());
			}
			// set the most recent valid value of modifiedTime
			if (old.getModifiedTime() > 0 && fetch.getModifiedTime() == 0)
			{
				result.setModifiedTime(old.getModifiedTime());
			}
		}

		switch (fetch.getStatus())
		{ 
			case CrawlDatum.STATUS_FETCH_SUCCESS: 

				result.setStatus(CrawlDatum.STATUS_DB_FETCHED);
				//result.setNextFetchTime();
				break;

			case CrawlDatum.STATUS_FETCH_REDIR_TEMP:
				result.setStatus(CrawlDatum.STATUS_DB_REDIR_TEMP);
				//result.setNextFetchTime();
				break;
			case CrawlDatum.STATUS_FETCH_REDIR_PERM:
				result.setStatus(CrawlDatum.STATUS_DB_REDIR_PERM);
				//result.setNextFetchTime();
				break;
			case CrawlDatum.STATUS_FETCH_RETRY:
				if (fetch.getRetriesSinceFetch() < retryMax)
				{
					result.setStatus(CrawlDatum.STATUS_DB_UNFETCHED);
				} else
				{
					result.setStatus(CrawlDatum.STATUS_DB_GONE);
				}
				break;
			case CrawlDatum.STATUS_LINKED: 
				if (old != null)
				{ // if old exists
					result.set(old); // use it
				} else
				{
					result.setStatus(CrawlDatum.STATUS_DB_UNFETCHED);
				}
				break;
			case CrawlDatum.STATUS_FETCH_GONE:
				result.setStatus(CrawlDatum.STATUS_DB_GONE);
				break;

			default:
				throw new RuntimeException("Unknown status: "
						+ fetch.getStatus() + " " + key);
		}
 
		result.getMetaData().remove(Spider.GENERATE_TIME_KEY);
		context.write(key, result.datum);
	}

}
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

import kim.spider.fetcher.FetcherSmart;
import kim.spider.io.MapAvroFile;
import kim.spider.parse.ParseSegment;
import kim.spider.util.SpiderConfiguration;
import kim.spider.util.SpiderJob;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.StringUtils;

public class Crawl {
	public static final Log			LOG		= LogFactory.getLog(Crawl.class);

	public static Configuration	conf	= null;

	public Crawl() {
		conf = SpiderConfiguration.create();
	}

	/* Perform complete crawling and indexing given a set of root urls. */
	public static void main(String args[]) throws Exception {
		// Crawl cr = new Crawl();
		conf = SpiderConfiguration.create();
		JobConf job = new SpiderJob(conf);

		Path dir = new Path("iphone");

		Path crawlDb = null;
		Path seg = null;
		int threads = job.getInt("fetcher.threads.fetch", 10);

		FileSystem fs = FileSystem.get(job);
		Injector injector = new Injector(conf);
		GeneratorSmart generator = new GeneratorSmart(conf);
		FetcherSmart fetcher = new FetcherSmart(conf);
		CrawlDb crawlDbTool = new CrawlDb(conf);
		ParseSegment parse = new ParseSegment(conf);
		crawlDb = new Path(dir + "/crawldb");
		seg = new Path(dir + "/segments");
		injector.inject(crawlDb, new Path(dir, "iphone.seed"));
		try {
			while (true) {

				Path[] segments = null;
				segments = generator.generate(crawlDb, seg, 1,
						System.currentTimeMillis(), false);
				if (segments == null) {
					LOG.info("Stopping dute no more URLs to fetch.");
					break;
					// return;
				}
				for (Path segment : segments) {
					fetcher.fetch(segment, threads); // fetch it
					parse.parse(segment);
				}
				crawlDbTool.update(crawlDb, segments); // update
			}
		} catch (Exception e) {
			if (LOG.isFatalEnabled())

				LOG.fatal("in CrawlInfo main() Exception "
						+ StringUtils.stringifyException(e) + "\n");
			return;
		}

		LOG.info("\r\nAppstore info is here:\n");
		for (FileStatus path : fs.listStatus(seg)) {
			for (FileStatus info : fs.listStatus(new Path(path.getPath().toString()
					+ "/AppInfo"))) {
				MapAvroFile.Reader reader = new MapAvroFile.Reader(fs, info.getPath()
						.toString(), conf);
				while (reader.hasNext()) {

					LOG.info(reader.next().value().toString());
				}
				reader.close();
			}
		}
		LOG.info("Crawl is done!\n");
	}

}

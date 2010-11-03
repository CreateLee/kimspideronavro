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
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import kim.spider.avro.mapreduce.AvroJob;
import kim.spider.avro.mapreduce.input.AvroPairInputFormat;
import kim.spider.avro.mapreduce.output.AvroMapOutputFormat;
import kim.spider.avro.mapreduce.output.AvroPairOutputFormat;
import kim.spider.net.BasicURLNormalizer;
import kim.spider.util.SpiderConfiguration;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This class takes a flat file of URLs and adds them to the of pages to be
 * crawled. Useful for bootstrapping the system. The URL files contain one URL
 * per line, optionally followed by custom metadata separated by tabs with the
 * metadata key separated from the corresponding value by '='. <br>
 * Note that some metadata keys are reserved : <br>
 * - <i>nutch.score</i> : allows to set a custom score for a specific URL <br>
 * - <i>nutch.fetchInterval</i> : allows to set a custom fetch interval for a
 * specific URL <br>
 * e.g. http://www.nutch.org/ \t nutch.score=10 \t nutch.fetchInterval=2592000
 * \t userType=open_source
 **/
public class Injector extends Configured implements Tool {
	public static final Log	LOG	= LogFactory.getLog(Injector.class);

	/** Normalize and filter injected urls. */
	public static class InjectMapper extends
			Mapper<WritableComparable, Text, String, kim.spider.schema.CrawlDatum> {

		private int									interval;
		private float								scoreInjected;
		private Configuration				jobConf;
		private long								curTime;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			jobConf = context.getConfiguration();
			interval = jobConf.getInt("db.fetch.interval.default", 2592000);
			scoreInjected = jobConf.getFloat("db.score.injected", 1.0f);
			curTime = jobConf.getLong("injector.current.time",
					System.currentTimeMillis());
		}

		@Override
		protected void map(WritableComparable key, Text value, Context context)
				throws IOException, InterruptedException {

			String url = value.toString();
			float customScore = -1f;
			int customInterval = interval;
			java.util.Map<java.lang.CharSequence, java.lang.CharSequence> metaData = new HashMap<java.lang.CharSequence, java.lang.CharSequence>();
			Map<java.lang.CharSequence, java.lang.CharSequence> extendData = new HashMap<java.lang.CharSequence, java.lang.CharSequence>();
			if (url.indexOf("\t") != -1) {
				String[] splits = url.split("\t");
				url = splits[0];
				for (int s = 1; s < splits.length; s++) {
					// find separation between name and value
					int indexEquals = splits[s].indexOf("=");
					if (indexEquals == -1) {
						// skip anything without a =
						continue;
					}
					String metaname = splits[s].substring(0, indexEquals);
					String metavalue = splits[s].substring(indexEquals + 1);
					if (!metaname.equals(CrawlDatum.PARSE_CLASS)) {
						metaData.put(metaname, metavalue);
					} else
						extendData.put(metaname, metavalue);
				}
				try {
					url = new BasicURLNormalizer().normalize(url);
				} catch (MalformedURLException e) {
					LOG.warn("Skipping " + url + ":" + e);
					url = null;
				}
				if (url != null) { // if it passes
					CrawlDatum datum = new CrawlDatum(CrawlDatum.STATUS_INJECTED,
							customInterval);
					datum.setFetchTime(curTime);
					if (customScore != -1)
						datum.setScore(customScore);
					else {
						datum.setScore(scoreInjected);
					}
					datum.setMetaData(metaData);
					datum.setExtendData(extendData);
					context.write(url, datum.datum);
				}
			}
		}

	}

	/** Combine multiple new entries for a url. */
	public static class InjectReducer extends
			Reducer<String, kim.spider.schema.CrawlDatum, String, kim.spider.schema.CrawlDatum> {

		private CrawlDatum	old				= new CrawlDatum();
		private CrawlDatum	injected	= new CrawlDatum();

		@Override
		protected void reduce(String key, Iterable<kim.spider.schema.CrawlDatum> values, Context context)
				throws IOException, InterruptedException {
			boolean oldSet = false;
			for (kim.spider.schema.CrawlDatum val : values) {
				
				CrawlDatum cur = new CrawlDatum(val);
				if (cur.getStatus() == CrawlDatum.STATUS_INJECTED) {
					injected.set(cur);
					injected.setStatus(CrawlDatum.STATUS_DB_UNFETCHED);
				} else {
					old.set(cur);
					oldSet = true;
				}
			}
			CrawlDatum res = null;
			if (oldSet)
				res = old; // don't overwrite existing value
			else
				res = injected;
			context.write(key, res.datum);
		}
	}

	public Injector() {
	}

	public Injector(Configuration conf) {
		setConf(conf);
	}

	public void inject(Path crawlDb, Path urlDir) throws IOException, IllegalStateException, InterruptedException, ClassNotFoundException {

		if (LOG.isInfoEnabled()) {
			LOG.info("Injector: starting");
			LOG.info("Injector: crawlDb: " + crawlDb);
			LOG.info("Injector: urlDir: " + urlDir);
		}

		Path tempDir = new Path(getConf().get("mapred.temp.dir", ".")
				+ "/inject-temp-"
				+ Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

		// map text input file to a <url,CrawlDatum> file
		if (LOG.isInfoEnabled()) {
			LOG.info("Injector: Converting injected urls to crawl db entries.");
		}
		Job sortJob = AvroJob.getAvroJob(getConf());
		sortJob.setJobName("inject " + urlDir);
		FileInputFormat.addInputPath(sortJob, urlDir);
		sortJob.setMapperClass(InjectMapper.class);
		FileOutputFormat.setOutputPath(sortJob, tempDir);
		sortJob.setOutputKeyClass(String.class);
		sortJob.setOutputValueClass(kim.spider.schema.CrawlDatum.class);
		sortJob.setOutputFormatClass(AvroPairOutputFormat.class);

		if (sortJob.waitForCompletion(true)) {
			// merge with existing crawl db
			if (LOG.isInfoEnabled()) {
				LOG.info("Injector: Merging injected urls into crawl db.");
			}
			Job job = AvroJob.getAvroJob(getConf());
			job.setJobName("crawldb " + crawlDb);

			Path current = new Path(crawlDb, CrawlDb.CURRENT_NAME);
			FileInputFormat.addInputPath(job, tempDir);
			if (FileSystem.get(getConf()).exists(current)) {
				FileInputFormat.addInputPath(job, current);
			}
			job.setInputFormatClass(AvroPairInputFormat.class);
			job.setReducerClass(InjectReducer.class);
			job.setMapperClass(CrawlDbFilter.class);
			Path newCrawlDb = new Path(crawlDb, Integer.toString(new Random()
			.nextInt(Integer.MAX_VALUE)));
			FileOutputFormat.setOutputPath(job, newCrawlDb);
			job.setOutputFormatClass(AvroMapOutputFormat.class);
			job.setOutputKeyClass(String.class);
			job.setOutputValueClass(kim.spider.schema.CrawlDatum.class);
			job.waitForCompletion(true);
			
			CrawlDb.install(job, crawlDb);

			// clean up
			FileSystem fs = FileSystem.get(getConf());
			fs.delete(tempDir, true);
			if (LOG.isInfoEnabled()) {
				LOG.info("Injector: done");
			}
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner
				.run(SpiderConfiguration.create(), new Injector(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: Injector <crawldb> <url_dir>");
			return -1;
		}
		try {
			inject(new Path(args[0]), new Path(args[1]));
			return 0;
		} catch (Exception e) {
			LOG.fatal("Injector: " + StringUtils.stringifyException(e));
			return -1;
		}
	}

}

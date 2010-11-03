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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;

import kim.spider.avro.mapreduce.AvroJob;
import kim.spider.avro.mapreduce.input.AvroPairInputFormat;
import kim.spider.avro.mapreduce.output.AvroMapOutputFormat;
import kim.spider.util.HadoopFSUtil;
import kim.spider.util.LockUtil;
import kim.spider.util.SpiderConfiguration;

import org.apache.avro.util.Utf8;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This class takes the output of the fetcher and updates the crawldb
 * accordingly.
 */
public class CrawlDb extends Configured implements Tool {
	public static final Log LOG = LogFactory.getLog(CrawlDb.class);

	public static final String CRAWLDB_ADDITIONS_ALLOWED = "db.update.additions.allowed";

	public static final String CURRENT_NAME = "current";

	public static final String LOCK_NAME = ".locked";

	public CrawlDb() {
	}

	public CrawlDb(Configuration conf) {
		setConf(conf);
	}

	public void update(Path crawlDb, Path[] segments) throws IOException, InterruptedException, ClassNotFoundException {
		boolean additionsAllowed = getConf().getBoolean(
				CRAWLDB_ADDITIONS_ALLOWED, true);
		update(crawlDb, segments, additionsAllowed, false);
	}

	public void update(Path crawlDb, Path[] segments, boolean additionsAllowed, boolean force)
			throws IOException, InterruptedException, ClassNotFoundException {
		FileSystem fs = FileSystem.get(getConf());
		Path lock = new Path(crawlDb, LOCK_NAME);
		LockUtil.createLockFile(fs, lock, force);
		if (LOG.isInfoEnabled()) {
			LOG.info("CrawlDb update: starting");
			LOG.info("CrawlDb update: db: " + crawlDb);
			LOG.info("CrawlDb update: segments: " + Arrays.asList(segments));
			LOG.info("CrawlDb update: additions allowed: " + additionsAllowed);
		}
		
		Job job = CrawlDb.createJob(getConf(), crawlDb);
		job.getConfiguration().setBoolean(CRAWLDB_ADDITIONS_ALLOWED, additionsAllowed);
		for (int i = 0; i < segments.length; i++) {
			Path fetch = new Path(segments[i], CrawlDatum.FETCH_DIR_NAME);
			Path parse = new Path(segments[i], CrawlDatum.PARSE_DIR_NAME);
			if (fs.exists(fetch)) {
				FileInputFormat.addInputPath(job, fetch);
			} 
			if( fs.exists(parse))
			{
				FileInputFormat.addInputPath(job, parse);
			}
			else {
				LOG.info(" - skipping invalid segment " + segments[i]);
			}
		}

		if (LOG.isInfoEnabled()) {
			LOG.info("CrawlDb update: Merging segment data into db.");
		}
		try {
			job.waitForCompletion(true);
		} catch (IOException e) {
			LockUtil.removeLockFile(fs, lock);
			Path outPath = FileOutputFormat.getOutputPath(job);
			if (fs.exists(outPath))
				fs.delete(outPath, true);
			throw e;
		} catch (InterruptedException e) {
			LockUtil.removeLockFile(fs, lock);
			Path outPath = FileOutputFormat.getOutputPath(job);
			if (fs.exists(outPath))
				fs.delete(outPath, true);
			throw e;
		} catch (ClassNotFoundException e) {
			LockUtil.removeLockFile(fs, lock);
			Path outPath = FileOutputFormat.getOutputPath(job);
			if (fs.exists(outPath))
				fs.delete(outPath, true);
			throw e;
		}

		CrawlDb.install(job, crawlDb);
		if (LOG.isInfoEnabled()) {
			LOG.info("CrawlDb update: done");
		}
	}

	public static Job createJob(Configuration config, Path crawlDb)
			throws IOException {
		Path newCrawlDb = new Path(crawlDb, Integer.toString(new Random()
				.nextInt(Integer.MAX_VALUE)));

		Job job = AvroJob.getAvroJob(config);
		job.setJobName("crawldb " + crawlDb);

		Path current = new Path(crawlDb, CURRENT_NAME);
		if (FileSystem.get(config).exists(current)) {
			FileInputFormat.addInputPath(job, current);
		}
		job.setInputFormatClass(AvroPairInputFormat.class);

		job.setMapperClass(CrawlDbFilter.class);
		job.setReducerClass(CrawlDbReducer.class);

		FileOutputFormat.setOutputPath(job, newCrawlDb);
		job.setOutputFormatClass(AvroMapOutputFormat.class);
		job.setOutputKeyClass(String.class);
		job.setOutputValueClass(kim.spider.schema.CrawlDatum.class);

		return job;
	}

	public static void install(Job job, Path crawlDb) throws IOException {
		Path newCrawlDb = FileOutputFormat.getOutputPath(job);
		FileSystem fs = FileSystem.get(job.getConfiguration());
		Path old = new Path(crawlDb, "old");
		Path current = new Path(crawlDb, CURRENT_NAME);
		if (fs.exists(current)) {
			if (fs.exists(old))
				fs.delete(old, true);
			fs.rename(current, old);
		}
		fs.mkdirs(crawlDb);
		fs.rename(newCrawlDb, current);
		if (fs.exists(old))
			fs.delete(old, true);
		Path lock = new Path(crawlDb, LOCK_NAME);
		LockUtil.removeLockFile(fs, lock);
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(SpiderConfiguration.create(), new CrawlDb(),
				args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		if (args.length < 2) {
			System.err
					.println("Usage: CrawlDb <crawldb> (-dir <segments> | <seg1> <seg2> ...) [-force] [-noAdditions]");
			System.err.println("\tcrawldb\tCrawlDb to update");
			System.err
					.println("\t-dir segments\tparent directory containing all segments to update from");
			System.err
					.println("\tseg1 seg2 ...\tlist of segment names to update from");
			System.err
					.println("\t-force\tforce update even if CrawlDb appears to be locked (CAUTION advised)");
			System.err
					.println("\t-noAdditions\tonly update already existing URLs, don't add any newly discovered URLs");
			return -1;
		}
		boolean force = false;
		final FileSystem fs = FileSystem.get(getConf());
		boolean additionsAllowed = getConf().getBoolean(
				CRAWLDB_ADDITIONS_ALLOWED, true);
		HashSet<Path> dirs = new HashSet<Path>();
		for (int i = 1; i < args.length; i++) {
			if (args[i].equals("-force")) {
				force = true;
			} else if (args[i].equals("-noAdditions")) {
				additionsAllowed = false;
			} else if (args[i].equals("-dir")) {
				FileStatus[] paths = fs.listStatus(new Path(args[++i]),
						HadoopFSUtil.getPassDirectoriesFilter(fs));
				dirs.addAll(Arrays.asList(HadoopFSUtil.getPaths(paths)));
			} else {
				dirs.add(new Path(args[i]));
			}
		}
		try {
			update(new Path(args[0]), dirs.toArray(new Path[dirs.size()]), additionsAllowed, force);
			return 0;
		} catch (Exception e) {
			LOG.fatal("CrawlDb update: " + StringUtils.stringifyException(e));
			return -1;
		}
	}
}

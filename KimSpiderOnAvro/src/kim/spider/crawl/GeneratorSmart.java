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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

import kim.spider.avro.mapreduce.AvroJob;
import kim.spider.avro.mapreduce.input.AvroPairInputFormat;
import kim.spider.avro.mapreduce.output.AvroMapOutputFormat;
import kim.spider.avro.mapreduce.output.AvroPairOutputFormat;
import kim.spider.avro.mapreduce.output.GeneratorOutputFormat;
import kim.spider.metadata.Spider;
import kim.spider.util.LockUtil;
import kim.spider.util.SpiderConfiguration;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/* its for several segments in one go. Unlike in the initial version
 * (OldGenerator), the IP resolution is done ONLY on the entries which have been
 * selected for fetching. The URLs are partitioned by IP, domain or host within
 * a segment. We can chose separately how to count the URLS i.e. by domain or
 * host to limit the entries.
 **/
public class GeneratorSmart extends Configured implements Tool {

	public static final Log			LOG														= LogFactory
																																.getLog(GeneratorSmart.class);

	public static final String	GENERATE_UPDATE_CRAWLDB				= "generate.update.crawldb";
	public static final String	GENERATOR_DELAY								= "crawl.gen.delay";
	public static final String	GENERATOR_CUR_TIME						= "generate.curTime";
	public static final String	GENERATOR_MAX_NUM_SEGMENTS		= "generate.max.num.segments";
	public static final String	GENERATOR_COUNT_PER_SEGMENTS	= "generate.count.per.segments";

	public static class SelectorEntry {
		public String												url;
		public kim.spider.schema.CrawlDatum	datum;
		public int													segnum;
	}

	/** Selects entries due for fetch. */
	public static class SelectorMapper extends
			Mapper<String, kim.spider.schema.CrawlDatum, Float, SelectorEntry> {
		private long					genTime	= System.currentTimeMillis();
		private long					curTime;

		private SelectorEntry	entry		= new SelectorEntry();
		private long					genDelay;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			curTime = context.getConfiguration().getLong(GENERATOR_CUR_TIME,
					System.currentTimeMillis());
			long time = context.getConfiguration().getLong(Spider.GENERATE_TIME_KEY,
					0L);
			if (time > 0)
				genTime = time;
			genDelay = context.getConfiguration().getLong(GENERATOR_DELAY, 7L) * 3600L * 24L * 1000L;
		}

		@Override
		protected void map(String key, kim.spider.schema.CrawlDatum value,
				Context context) throws IOException, InterruptedException {

			CrawlDatum crawlDatum = new CrawlDatum(value);

			if (crawlDatum.getStatus() == CrawlDatum.STATUS_DB_FETCHED)
				return;
			if (crawlDatum.getStatus() == CrawlDatum.STATUS_DB_GONE)
				return;
			if (key.toString()
					.matches("http://www.qidian.com/BookReader/\\d+,0.aspx"))
				return;
			if (crawlDatum.getMetaData().get(Spider.GENERATE_TIME_KEY) != null) {
				String oldgen = crawlDatum.getMetaData().get(Spider.GENERATE_TIME_KEY)
						.toString();

				if (oldgen != null) { // awaiting fetch & update
					long oldGenTime = Long.parseLong(oldgen);
					if (oldGenTime + genDelay > curTime) // still wait for
						// update
						return;
				}
			}

			// record generation time
			crawlDatum.setMeta(Spider.GENERATE_TIME_KEY, String.valueOf(genTime));
			entry.datum = crawlDatum.datum;
			entry.url = key;
			context.write(RandomUtils.nextFloat(), entry);
		}
	}

	public static class SelectorReducer extends
			Reducer<Float, SelectorEntry, Float, SelectorEntry> {
		private long																			count							= 0;
		private long																			limit;							
		int																								currentsegmentnum	= 1;
		//private AvroMultipleOutputs<Float, SelectorEntry>	mos;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			limit = context.getConfiguration().getInt(GENERATOR_COUNT_PER_SEGMENTS,
					5000);
			//mos = new AvroMultipleOutputs<Float, SelectorEntry>(context);
		}

		@Override
		protected void reduce(Float key, Iterable<SelectorEntry> values,
				Context context) throws IOException, InterruptedException {
			for (SelectorEntry entry : values) {

				if (count >= limit) {
					// do we have any segments left?
					count = 0;
					currentsegmentnum++;
				}

				entry.segnum = currentsegmentnum;
				//mos.write(key, entry, generateFileNameForKeyValue(key, entry));
				context.write(key, entry);
				count++;
			}
		}

//		@Override
//		protected void cleanup(Context context) throws IOException,
//				InterruptedException {
//			mos.close();
//		}
	}

	public static class SelectorInverseMapper extends
			Mapper<Float, SelectorEntry, String, SelectorEntry> {
		int	numUrls	= 0;

		@Override
		protected void map(Float key, SelectorEntry value, Context context)
				throws IOException, InterruptedException {
			value.segnum = numUrls;
			numUrls++;
			context.write(value.url.toString(), value);
		}

	}

	public static class AveragePartition extends
			Partitioner<String, SelectorEntry> {
		@Override
		public int getPartition(String key, SelectorEntry value, int numPartitions) {

			return value.segnum % numPartitions;
		}
	}

	public static class PartitionReducer extends
			Reducer<String, SelectorEntry, String, kim.spider.schema.CrawlDatum> {

		protected void reduce(String key, Iterable<SelectorEntry> values,
				Context context) throws IOException, InterruptedException {
			for (SelectorEntry value : values) {
				context.write(key, value.datum);
			}
		}

	}

	/**
	 * Update the CrawlDB so that the next generate won't include the same URLs.
	 */
	public static class CrawlDbUpdateMapper
			extends
			Mapper<String, kim.spider.schema.CrawlDatum, String, kim.spider.schema.CrawlDatum> {
	}

	public static class CrawlDbUpdateReducer
			extends
			Reducer<String, kim.spider.schema.CrawlDatum, String, kim.spider.schema.CrawlDatum> {
		long	generateTime;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			generateTime = context.getConfiguration().getLong(
					Spider.GENERATE_TIME_KEY, 0L);
		}

		private CrawlDatum	orig		= new CrawlDatum();
		private long				genTime	= 0;

		@Override
		protected void reduce(String key,
				Iterable<kim.spider.schema.CrawlDatum> values, Context context)
				throws IOException, InterruptedException {
			for (kim.spider.schema.CrawlDatum v : values) {
				CrawlDatum val = new CrawlDatum(v);
				if (val.getMetaData().containsKey(Spider.GENERATE_TIME_KEY)) {
					genTime = Long.parseLong(val.getMetaData()
							.get(Spider.GENERATE_TIME_KEY).toString());
					if (genTime != generateTime) {
						orig.set(val);
						genTime = 0;
						continue;
					}
				} else {
					orig.set(val);
				}
			}
			if (genTime != 0) {
				orig.getMetaData().put(Spider.GENERATE_TIME_KEY,
						String.valueOf(genTime));
			}
			context.write(key, orig.datum);
		}
	}

	public GeneratorSmart() {
	}

	public GeneratorSmart(Configuration conf) {
		setConf(conf);
	}

	/**
	 * Generate fetchlists in one or more segments. Whether to filter URLs or not
	 * is read from the crawl.generate.filter property in the configuration files.
	 * If the property is not found, the URLs are filtered. Same for the
	 * normalisation.
	 * 
	 * @param dbDir
	 *          Crawl database directory
	 * @param segments
	 *          Segments directory
	 * @param numLists
	 *          Number of reduce tasks
	 * @param curTime
	 *          Current time in milliseconds
	 * 
	 * @return Path to generated segment or null if no entries were selected
	 * 
	 * @throws IOException
	 *           When an I/O error occurs
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public Path[] generate(Path dbDir, Path segments, int numLists, long curTime,
			boolean force) throws IOException, InterruptedException,
			ClassNotFoundException {
		getConf().set("mapred.temp.dir", "d:/tmp");
		Path tempDir = new Path(getConf().get("mapred.temp.dir", ".")
				+ "/generate-temp-" + System.currentTimeMillis());

		Path lock = new Path(dbDir, CrawlDb.LOCK_NAME);
		FileSystem fs = FileSystem.get(getConf());
		LockUtil.createLockFile(fs, lock, force);

		LOG.info("Generator: Selecting best-scoring urls due for fetch.");
		LOG.info("Generator: starting");

		Job job = AvroJob.getAvroJob(getConf());
		if (numLists == -1) { // for politeness make
			numLists = job.getNumReduceTasks(); // a partition per fetch task
		}
		if ("local".equals(job.getConfiguration().get("mapred.job.tracker"))
				&& numLists != 1) {
			// override
			LOG.info("Generator: jobtracker is 'local', generating exactly one partition.");
			numLists = 1;
		}
		LOG.info("Generator: with " + numLists + " partition.");
		job.getConfiguration().setLong(GENERATOR_CUR_TIME, curTime);
		// record real generation time
		long generateTime = System.currentTimeMillis();
		job.getConfiguration().setLong(Spider.GENERATE_TIME_KEY, generateTime);

		FileInputFormat.addInputPath(job, new Path(dbDir, CrawlDb.CURRENT_NAME));
		job.setInputFormatClass(AvroPairInputFormat.class);

		job.setMapperClass(SelectorMapper.class);
		job.setReducerClass(SelectorReducer.class);

		FileOutputFormat.setOutputPath(job, tempDir);
		//job.setOutputFormatClass(AvroPairOutputFormat.class);
		job.setOutputFormatClass(GeneratorOutputFormat.class);
		job.setOutputKeyClass(Float.class);
		job.setOutputValueClass(SelectorEntry.class);
		// AvroMultipleOutputs.addNamedOutput(job, "seq",
		// AvroPairOutputFormat.class, Float.class, SelectorEntry.class);
		try {
			job.waitForCompletion(true);
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}

		// read the subdirectories generated in the temp
		// output and turn them into segments
		List<Path> generatedSegments = new ArrayList<Path>();

		FileStatus[] status = fs.listStatus(tempDir);
		try {
			for (FileStatus stat : status) {
				Path subfetchlist = stat.getPath();
				if (!subfetchlist.getName().startsWith("fetchlist-"))
					continue;
				// start a new partition job for this segment
				Path newSeg = partitionSegment(fs, segments, subfetchlist, numLists);

				fs.createNewFile(new Path(newSeg, "generatored"));
				generatedSegments.add(newSeg);
			}
		} catch (Exception e) {
			LOG.warn("Generator: exception while partitioning segments, exiting ...");
			fs.delete(tempDir, true);
			return null;
		}

		if (generatedSegments.size() == 0) {
			LOG.warn("Generator: 0 records selected for fetching, exiting ...");
			LockUtil.removeLockFile(fs, lock);
			fs.delete(tempDir, true);
			return null;
		}

		if (getConf().getBoolean(GENERATE_UPDATE_CRAWLDB, false)) {
			// update the db from tempDir
			Path tempDir2 = new Path(getConf().get("mapred.temp.dir", ".")
					+ "/generate-temp-" + System.currentTimeMillis());

			job = AvroJob.getAvroJob(getConf());
			job.setJobName("generate: updatedb " + dbDir);
			job.getConfiguration().setLong(Spider.GENERATE_TIME_KEY, generateTime);
			for (Path segmpaths : generatedSegments) {
				Path subGenDir = new Path(segmpaths, CrawlDatum.GENERATE_DIR_NAME);
				FileInputFormat.addInputPath(job, subGenDir);
			}
			FileInputFormat.addInputPath(job, new Path(dbDir, CrawlDb.CURRENT_NAME));
			job.setInputFormatClass(AvroPairInputFormat.class);
			job.setMapperClass(CrawlDbUpdateMapper.class);
			// job.setReducerClass(CrawlDbUpdater.class);
			job.setOutputFormatClass(AvroMapOutputFormat.class);
			job.setOutputKeyClass(String.class);
			job.setOutputValueClass(kim.spider.schema.CrawlDatum.class);
			FileOutputFormat.setOutputPath(job, tempDir2);
			try {
				job.waitForCompletion(true);
				CrawlDb.install(job, dbDir);
			} catch (IOException e) {
				LockUtil.removeLockFile(fs, lock);
				fs.delete(tempDir, true);
				fs.delete(tempDir2, true);
				throw e;
			}
			fs.delete(tempDir2, true);
		}

		LockUtil.removeLockFile(fs, lock);
		fs.delete(tempDir, true);
		
		if (LOG.isInfoEnabled()) {
			LOG.info("Generator: done.");
		}
		Path[] patharray = new Path[generatedSegments.size()];
		return generatedSegments.toArray(patharray);
	}

	private Path partitionSegment(FileSystem fs, Path segmentsDir, Path inputDir,
			int numLists) throws IOException, InterruptedException,
			ClassNotFoundException {
		// invert again, partition by host/domain/IP, sort by url hash
		if (LOG.isInfoEnabled()) {
			LOG.info("Generator: Partitioning selected urls for politeness:"
					+ inputDir);
		}
		Path segment = new Path(segmentsDir, generateSegmentName());
		Path output = new Path(segment, CrawlDatum.GENERATE_DIR_NAME);

		LOG.info("Generator: segment: " + segment + " with " + numLists
				+ " Fetchers");

		Job job = AvroJob.getAvroJob(getConf());
		job.setJobName("generate: partition " + segment);
		job.getConfiguration().setInt("partition.url.seed", new Random().nextInt());

		FileInputFormat.addInputPath(job, inputDir);
		job.setInputFormatClass(AvroPairInputFormat.class);

		job.setMapperClass(SelectorInverseMapper.class);
		job.setPartitionerClass(AveragePartition.class);
		job.setMapOutputKeyClass(String.class);
		job.setMapOutputValueClass(SelectorEntry.class);
		job.setReducerClass(PartitionReducer.class);
		job.setNumReduceTasks(numLists);

		FileOutputFormat.setOutputPath(job, output);
		job.setOutputFormatClass(AvroPairOutputFormat.class);
		job.setOutputKeyClass(String.class);
		job.setOutputValueClass(kim.spider.schema.CrawlDatum.class);

		job.waitForCompletion(true);
		return segment;
	}

	private static SimpleDateFormat	sdf	= new SimpleDateFormat("yyyyMMddHHmmss");

	public static synchronized String generateSegmentName() {
		try {
			Thread.sleep(1000);
		} catch (Throwable t) {
		}
		;
		return sdf.format(new Date(System.currentTimeMillis()));
	}

	/**
	 * Generate a fetchlist from the crawldb.
	 */
	public static void main(String args[]) throws Exception {
		int res = ToolRunner.run(SpiderConfiguration.create(),
				new GeneratorSmart(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		if (args.length < 2) {
			System.out
					.println("Usage: Generator <crawldb> <segments_dir> [-force] [-numFetchers numFetchers]");
			return -1;
		}

		Path dbDir = new Path(args[0]);
		Path segmentsDir = new Path(args[1]);
		long curTime = System.currentTimeMillis();
		int numFetchers = -1;
		boolean force = false;

		for (int i = 2; i < args.length; i++) {
			if ("-numFetchers".equals(args[i])) {
				numFetchers = Integer.parseInt(args[i + 1]);
				i++;
			} else if ("-force".equals(args[i])) {
				force = true;
			}

		}

		try {
			Path[] segs = generate(dbDir, segmentsDir, numFetchers, curTime, force);
			if (segs == null)
				return -1;
		} catch (Exception e) {
			LOG.fatal("Generator: " + StringUtils.stringifyException(e));
			return -1;
		}
		return 0;
	}
}

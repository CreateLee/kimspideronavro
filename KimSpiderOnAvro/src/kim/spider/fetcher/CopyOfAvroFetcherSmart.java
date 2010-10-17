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

package kim.spider.fetcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import kim.spider.avro.mapreduce.AvroJob;
import kim.spider.avro.mapreduce.MultithreadedBlockMapper;
import kim.spider.avro.mapreduce.MultithreadedBlockMapper.BlockMapper;
import kim.spider.avro.mapreduce.input.AvroPairInputFormat;
import kim.spider.avro.mapreduce.output.FetcherOutputFormat;
import kim.spider.crawl.CrawlDatum;
import kim.spider.io.SpiderData;
import kim.spider.metadata.Spider;
import kim.spider.protocol.Content;
import kim.spider.protocol.Protocol;
import kim.spider.protocol.ProtocolFactory;
import kim.spider.protocol.ProtocolOutput;
import kim.spider.protocol.ProtocolStatus;
import kim.spider.util.LogUtil;
import kim.spider.util.SpiderConfiguration;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 * 试用与对网站进行敏捷抓取，无任何设置的多线程并发快速访问网页
 * 只抓取有generatored标识的segment
 * 此抓取任务针对一个segments目录只允许启动一个任务（一个任务已经是分布式抓取）
 * 注：segments是包含segment的目录 
 */
/** The fetcher. Most of the work is done by plugins. */
public class CopyOfAvroFetcherSmart extends Configured implements Tool {

	public static final Log			LOG								= LogFactory
																										.getLog(CopyOfAvroFetcherSmart.class);

	public static final int			PERM_REFRESH_TIME	= 5;

	public static final String	CONTENT_REDIR			= "content";

	public static final String	PROTOCOL_REDIR		= "protocol";

	public static class InputFormat extends
			AvroPairInputFormat<String, CrawlDatum> {
		@Override
		public List<InputSplit> getSplits(JobContext job) throws IOException {
			// generate splits
			List<InputSplit> splits = new ArrayList<InputSplit>();
			List<FileStatus> files = listStatus(job);
			for (FileStatus file : files) {
				splits.add(new FileSplit(file.getPath(), 0, file.getLen(),
						(String[]) null));
			}
			// Save the number of input files for metrics/loadgen
			job.getConfiguration().setLong(NUM_INPUT_FILES, files.size());
			LOG.debug("Total # of splits: " + splits.size());
			return splits;
		}
	}

	public static class FetchMapper extends
			BlockMapper<String, kim.spider.schema.CrawlDatum, String, SpiderData> {

		private long						bytes;						// total bytes fetched
		private int							pages;						// total pages fetched
		private int							errors;					// total pages errored
		private ProtocolFactory	protocolFactory;
		Context									outer;
		private String					segmentName;

		protected void setup(Context context) throws IOException,
				InterruptedException {
			this.protocolFactory = new ProtocolFactory(context.getConfiguration());
			outer = context;
			this.segmentName = context.getConfiguration()
					.get(Spider.SEGMENT_NAME_KEY);
		}

		@Override
		protected void map(String key, kim.spider.schema.CrawlDatum val, Context context)
				throws IOException, InterruptedException {
			// url may be changed through redirects.
			CrawlDatum value = new CrawlDatum(val);
			try {
				if (LOG.isInfoEnabled()) {
					LOG.info("fetching " + key.toString());
				}
				
				Protocol protocol = this.protocolFactory.getProtocol(key.toString());
				ProtocolOutput output = protocol.getProtocolOutput(key.toString(),
						value);
				ProtocolStatus status = output.getStatus();
				Content content = output.getContent();
				
				
				switch (status.getCode()) {

				case ProtocolStatus.SUCCESS: // got a page
					output(key, value, content, status, CrawlDatum.STATUS_FETCH_SUCCESS);
					updateStatus(content.getContent().length);

					break;

				default:
					if (LOG.isWarnEnabled()) {
						LOG.warn("Unknown ProtocolStatus: " + status.getCode());
					}
					output(key, value, null, status, CrawlDatum.STATUS_FETCH_RETRY);
					logError(key.toString(), "" + status.getCode());
				}

			} catch (Throwable t) { // unexpected exception
				logError(key.toString(), t.toString());
				t.printStackTrace();
				output(key, value, null, null, CrawlDatum.STATUS_FETCH_RETRY);

			}
		}

		private void updateStatus(int bytesInPage) throws IOException {
			pages++;
			bytes += bytesInPage;
		}

		private void logError(String url, String message) {
			if (LOG.isInfoEnabled()) {
				LOG.info("fetch of " + url + " failed with: " + message);
			}

			errors++;
		}

		@Override
		public void BlockRecord() throws InterruptedException {
			if (currentValue != null)
				output(currentKey, new CrawlDatum(currentValue), null, null,
						CrawlDatum.STATUS_FETCH_RETRY);
		}

		private void output(String key, CrawlDatum datum, Content content,
				ProtocolStatus pstatus, int status) throws InterruptedException {

			datum.setStatus(status);
			datum.setFetchTime(System.currentTimeMillis());

			if (content != null) {

				// add segment to metadata
				content.addMetadata(Spider.SEGMENT_NAME_KEY, segmentName);
				content.addMetadata(Spider.FETCH_STATUS_KEY, String.valueOf(status));
				// 继承对应url的metadata，可以在parse中使用
				content.setExtend(datum.getExtendData());
			}

			try {
				outer.write(key, new SpiderData(datum.datum));

				if (content != null)
					outer.write(key, new SpiderData(content.datum));

			} catch (IOException e) {
				if (LOG.isFatalEnabled()) {
					e.printStackTrace(LogUtil.getFatalStream(LOG));
					LOG.fatal("fetcher caught:" + e.toString());
				}
			}
		}
	}

	/** Run the fetcher. */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(SpiderConfiguration.create(),
				new CopyOfAvroFetcherSmart(), args);
		System.exit(res);
	}

	public void fetch(Path segment, int threads) throws IOException,
			InterruptedException, ClassNotFoundException {

		if (LOG.isInfoEnabled()) {
			LOG.info("FetcherSmart: starting");
			LOG.info("FetcherSmart: segment: " + segment);
		}

		Job job = AvroJob.getAvroJob(getConf());
		job.setJobName("fetch " + segment);

		job.getConfiguration().setInt("fetcher.threads.fetch", threads);
		job.getConfiguration().set(Spider.SEGMENT_NAME_KEY, segment.getName());

		// for politeness, don't permit parallel execution of a single task
		job.setSpeculativeExecution(false);

		FileInputFormat.addInputPath(job, new Path(segment,
				CrawlDatum.GENERATE_DIR_NAME));
		job.setInputFormatClass(InputFormat.class);

		job.setMapperClass(MultithreadedBlockMapper.class);
		//job.setReducerClass(MOReduce.class);
		MultithreadedBlockMapper.setMapperClass(job, FetchMapper.class);
		MultithreadedBlockMapper.setNumberOfThreads(job, threads);

		FileOutputFormat.setOutputPath(job, segment);
		job.setMapOutputKeyClass(String.class);
		job.setMapOutputValueClass(SpiderData.class);
		//GenericAvroData.setGenericClass(job, kim.spider.schema.CrawlDatum.class,kim.spider.schema.Content.class);
//		job.setOutputKeyClass(String.class);
//		job.setOutputValueClass( kim.spider.schema.CrawlDatum.class);
		job.setOutputFormatClass(FetcherOutputFormat.class);
		// MultipleOutputs.addNamedOutput(job, CrawlDatum.FETCH_DIR_NAME,
		// AvroMapOutputFormat.class,String.class, CrawlDatum.class);
		
		job.waitForCompletion(true);
		if (LOG.isInfoEnabled()) {
			LOG.info("FetcherSmart: done");
		}
	}

	public static class MOReduce extends Reducer<String, SpiderData, String, Object> {
	
		@Override
		public void setup(Context context) {
		}

		@Override
		protected void reduce(String key, Iterable<SpiderData> values, Context context)
				throws IOException, InterruptedException {
			for (SpiderData value : values) {
				context.write(key, value);
			}
		}

	}

	public int run(String[] args) throws Exception {

		String usage = "Usage: Fetcher <segment> [-threads n]";

		if (args.length < 1) {
			System.err.println(usage);
			return -1;
		}

		Path segments = new Path(args[0]);

		int threads = getConf().getInt("fetcher.threads.fetch", 10);

		for (int i = 1; i < args.length; i++) { // parse command line
			if (args[i].equals("-threads")) { // found -threads option
				threads = Integer.parseInt(args[++i]);
			}
		}

		getConf().setInt("fetcher.threads.fetch", threads);
		FileSystem fs = FileSystem.get(getConf());
		try {
			for (FileStatus p : fs.listStatus(segments)) {
//				if (fs.exists(new Path(p.getPath(), "generatored"))
//						&& !fs.exists(new Path(p.getPath(), "fetched"))) {
					fetch(p.getPath(), threads);
					fs.createNewFile(new Path(p.getPath(), "fetched"));
					break;
//				}
			}
			return 0;
		} catch (Exception e) {
			LOG.fatal("Fetcher: " + StringUtils.stringifyException(e));
			return -1;
		}

	}
}


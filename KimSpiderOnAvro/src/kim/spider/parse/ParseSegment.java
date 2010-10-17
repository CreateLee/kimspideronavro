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

import java.io.IOException;
import java.util.Iterator;

import kim.spider.io.WritableList;
import kim.spider.metadata.Spider;
import kim.spider.protocol.Content;
import kim.spider.util.SpiderConfiguration;
import kim.spider.util.SpiderJob;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.htmlcleaner.HtmlCleaner;

/* Parse content in a segment. */
public class ParseSegment extends Configured implements Tool,
		Mapper<Text, Content, Text, WritableList>,
		Reducer<Text, WritableList, Text, WritableList> {

	public static final Log LOG = LogFactory.getLog(ParseSegment.class);

	public ParseSegment() {
		this(null);
	}

	public ParseSegment(Configuration conf) {
		super(conf);
	}

	public void configure(JobConf job) {
		setConf(job);
	}

	public void close() {
	}

	private Text newKey = new Text();

	public void map(Text key, Content content,
			OutputCollector<Text, WritableList> output, Reporter reporter)
			throws IOException {
		Text url = (Text) key;
		Parse parse = new ParserFactory().getParsers(url.toString(), content);
		WritableList pd = parse.parse(url.toString(), content);
		output.collect(key, pd);
	}

	public void reduce(Text key, Iterator<WritableList> values,
			OutputCollector<Text, WritableList> output, Reporter reporter)
			throws IOException {
		output.collect(key, (WritableList) values.next()); // collect first
															// value
	}

	public void parse(Path segment) throws IOException {

		if (LOG.isInfoEnabled()) {
			LOG.info("Parse: starting");
			LOG.info("Parse: segment: " + segment);
		}

		JobConf job = new SpiderJob(getConf());
		job.setJobName("parse " + segment);

		FileInputFormat.addInputPath(job, new Path(segment, Content.DIR_NAME));
		job.set(Spider.SEGMENT_NAME_KEY, segment.getName());
		//起点小说使用了HtmlCleaner
		job.setJarByClass(HtmlCleaner.class);
		job.setInputFormat(SequenceFileInputFormat.class);
		job.setMapperClass(ParseSegment.class);
		job.setReducerClass(ParseSegment.class);

		FileOutputFormat.setOutputPath(job, segment);
		job.setOutputFormat(ParseOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(WritableList.class);

		JobClient.runJob(job);
		if (LOG.isInfoEnabled()) {
			LOG.info("Parse: done");
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(SpiderConfiguration.create(),
				new ParseSegment(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {

		String usage = "Usage: ParseSegment segments";

		if (args.length == 0) {
			System.err.println(usage);
			System.exit(-1);
		}
		FileSystem fs = FileSystem.get(getConf());
		for (FileStatus p : fs.listStatus(new Path(args[0]))) {
			if (fs.exists(new Path(p.getPath(), "crawl_parse")))
				fs.delete(new Path(p.getPath(), "crawl_parse"), true);
			if (fs.exists(new Path(p.getPath(), "parse_data")))
				fs.delete(new Path(p.getPath(), "parse_data"), true);
			parse(p.getPath());
		}
		return 0;
	}
}

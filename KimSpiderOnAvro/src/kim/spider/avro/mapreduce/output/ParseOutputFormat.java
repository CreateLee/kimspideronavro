/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kim.spider.avro.mapreduce.output;

import java.io.IOException;

import kim.spider.crawl.CrawlDatum;
import kim.spider.crawl.GeneratorSmart.SelectorEntry;
import kim.spider.io.SpiderData;
import kim.spider.metadata.Spider;
import kim.spider.parse.Outlink;
import mobile.iphone.app.parse.IPhoneParse.AppInfo;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class ParseOutputFormat extends FileOutputFormat<String, SpiderData> {

	@Override
	public RecordWriter<String, SpiderData> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {

		final AvroMultipleOutputs<Float, SelectorEntry> mos = new AvroMultipleOutputs<Float, SelectorEntry>(
				job);
		mos.addNamedOutput("outlink", AvroPairOutputFormat.class, String.class,
				kim.spider.schema.CrawlDatum.class);
		mos.addNamedOutput("appinfo", AvroMapOutputFormat.class, String.class,
				AppInfo.class);

		return new RecordWriter<String, SpiderData>() {

			@Override
			public void write(String key, SpiderData value) throws IOException,
					InterruptedException {

				if (value.datum instanceof kim.spider.schema.Outlink) {
					Outlink ol = new Outlink();
					ol.datum = (kim.spider.schema.Outlink) value.datum;
					CrawlDatum datum = new CrawlDatum((int) CrawlDatum.STATUS_LINKED,
							ol.getFetchInterval());
					datum.setExtendData(ol.getExtend());
					mos.write("outlink", ol.getUrl(), datum.datum, Spider.PARSE_DIR_NAME
							+ "/");
				} else if (value.datum instanceof AppInfo) {
					mos.write("appinfo", key, value.datum, value.datum.getClass()
							.getSimpleName() + "/");
				}
			}

			@Override
			public void close(TaskAttemptContext context) throws IOException,
					InterruptedException {
				mos.close();

			}
		};
	}

	public void checkOutputSpecs(JobContext job)
			throws FileAlreadyExistsException, IOException {
		// Ensure that the output directory is set and not already there
		Path outDir = getOutputPath(job);
		if (outDir == null) {
			throw new InvalidJobConfException("Output directory not set.");
		}

		// get delegation token for outDir's file system
		TokenCache.obtainTokensForNamenodes(new Path[] { outDir },
				job.getConfiguration());

		// if (outDir.getFileSystem(job.getConfiguration()).exists(outDir)) {
		// throw new FileAlreadyExistsException("Output directory " + outDir
		// + " already exists");
		// }
	}
}

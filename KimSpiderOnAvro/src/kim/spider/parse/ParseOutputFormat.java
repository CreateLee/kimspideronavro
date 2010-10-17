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

// Commons Logging imports
import java.io.IOException;

import kim.spider.io.WritableList;
import kim.spider.metadata.Spider;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

/* Parse content in a segment. */
public class ParseOutputFormat implements OutputFormat<Text,WritableList> {
	private static final Log LOG = LogFactory.getLog(ParseOutputFormat.class);
	ParseRecordWriter prw = null;

	public void checkOutputSpecs(FileSystem fs, JobConf job) throws IOException {
		if (fs.exists(new Path(FileOutputFormat.getOutputPath(job), Spider.PARSE_DIR_NAME)))
			throw new IOException("Segment already parsed!");
	}

	public RecordWriter<Text,WritableList> getRecordWriter(FileSystem fs, JobConf job,
			String name, Progressable progress) throws IOException {

		if (prw == null)
			prw = new ParseRecordWriter(fs, job, name, progress);
		return prw;

	}

}

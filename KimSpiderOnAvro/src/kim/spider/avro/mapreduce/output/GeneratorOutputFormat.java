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

import kim.spider.crawl.GeneratorSmart.SelectorEntry;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class GeneratorOutputFormat extends
		FileOutputFormat<Float, SelectorEntry> {

	@Override
	public RecordWriter<Float, SelectorEntry> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		
		final AvroMultipleOutputs<Float, SelectorEntry> mos = new AvroMultipleOutputs<Float, SelectorEntry>(job);
		mos.addNamedOutput("fetchlist", AvroPairOutputFormat.class, Float.class, SelectorEntry.class);
		
		return new RecordWriter<Float, SelectorEntry>(){

			@Override
			public void write(Float key, SelectorEntry value) throws IOException,
					InterruptedException {
				
				mos.write("fetchlist",key, value, generateFileNameForKeyValue(key, value));
			}

			@Override
			public void close(TaskAttemptContext context) throws IOException,
					InterruptedException {
				mos.close();
				
			}
			
			String generateFileNameForKeyValue(Float key, SelectorEntry value) {
				return "fetchlist-" + value.segnum;
			}
		};
	}
}

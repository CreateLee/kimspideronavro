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

package kim.spider.avro.mapreduce.input;

import java.io.IOException;
import java.util.List;

import kim.spider.io.MapAvroFile;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


/** An {@link org.apache.hadoop.mapred.InputFormat} for sequence files. */
public class AvroPairInputFormat<K,V>
  extends FileInputFormat<K,V> {

	@Override
	public RecordReader<K, V> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return new AvroPairRecordReader<K, V>();
	}
	
	 @Override
	  protected List<FileStatus> listStatus(JobContext job
	                                        )throws IOException {

	    List<FileStatus> files = super.listStatus(job);
	    int len = files.size();
	    for(int i=0; i < len; ++i) {
	      FileStatus file = files.get(i);
	      if (file.isDirectory()) {     // it's a MapFile
	        Path p = file.getPath();
	        FileSystem fs = p.getFileSystem(job.getConfiguration());
	        // use the data file
	        files.set(i, fs.getFileStatus(new Path(p, MapAvroFile.DATA_FILE_NAME)));
	      }
	    }
	    return files;
	  }
	 
}

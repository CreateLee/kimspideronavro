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

import java.io.FileReader;
import java.io.IOException;

import org.apache.avro.file.DataFileReader;

import org.apache.avro.mapred.FsInput;
import org.apache.avro.mapred.Pair;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/** An {@link RecordReader} for Avro data files. */
public class AvroPairRecordReader<K, V> extends RecordReader<K, V> {

	private DataFileReader<Pair<K, V>>	reader;
	private long												start;
	private long												end;
	private K														key		= null;
	private V														value	= null;

	public float getProgress() throws IOException {
		if (end == start) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (getPos() - start) / (float) (end - start));
		}
	}

	public long getPos() throws IOException {
		return reader.previousSync();
	}

	public void close() throws IOException {
		reader.close();
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		FileSplit fileSplit = (FileSplit) split;
		reader = new DataFileReader<Pair<K, V>>(new FsInput(fileSplit.getPath(),
				context.getConfiguration()), new ReflectDatumReader<Pair<K, V>>());
		reader.sync(fileSplit.getStart()); // sync to start
		this.start = reader.previousSync();
		this.end = fileSplit.getStart() + split.getLength();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (!reader.hasNext() || reader.pastSync(end))
			return false;
		Pair<K, V> pair = reader.next();
		key = pair.key();
		value = pair.value();
		return true;
	}

	@Override
	public K getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return key;
	}

	@Override
	public V getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}

}

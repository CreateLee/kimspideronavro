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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * The MultipleOutputs class simplifies writing output data to multiple outputs
 * 
 * <p>
 * Case one: writing to additional outputs other than the job default output.
 * 
 * Each additional output, or named output, may be configured with its own
 * <code>OutputFormat</code>, with its own key class and with its own value
 * class.
 * 
 * <p>
 * Case two: to write data to different files provided by user
 * </p>
 * 
 * <p>
 * MultipleOutputs supports counters, by default they are disabled. The counters
 * group is the {@link AvroMultipleOutputs} class name. The names of the
 * counters are the same as the output name. These count the number records
 * written to each output name.
 * </p>
 * 
 * Usage pattern for job submission:
 * 
 * <pre>
 * 
 * Job job = new Job();
 * 
 * FileInputFormat.setInputPath(job, inDir);
 * FileOutputFormat.setOutputPath(job, outDir);
 * 
 * job.setMapperClass(MOMap.class);
 * job.setReducerClass(MOReduce.class);
 * ...
 * 
 * // Defines additional single text based output 'text' for the job
 * MultipleOutputs.addNamedOutput(job, "text", TextOutputFormat.class,
 * LongWritable.class, Text.class);
 * 
 * // Defines additional sequence-file based output 'sequence' for the job
 * MultipleOutputs.addNamedOutput(job, "seq",
 *   SequenceFileOutputFormat.class,
 *   LongWritable.class, Text.class);
 * ...
 * 
 * job.waitForCompletion(true);
 * ...
 * </pre>
 * <p>
 * Usage in Reducer:
 * 
 * <pre>
 * <K, V> String generateFileName(K k, V v) {
 *   return k.toString() + "_" + v.toString();
 * }
 * 
 * public class MOReduce extends
 *   Reducer&lt;WritableComparable, Writable,WritableComparable, Writable&gt; {
 * private MultipleOutputs mos;
 * public void setup(Context context) {
 * ...
 * mos = new MultipleOutputs(context);
 * }
 * 
 * public void reduce(WritableComparable key, Iterator&lt;Writable&gt; values,
 * Context context)
 * throws IOException {
 * ...
 * mos.write("text", , key, new Text("Hello"));
 * mos.write("seq", LongWritable(1), new Text("Bye"), "seq_a");
 * mos.write("seq", LongWritable(2), key, new Text("Chau"), "seq_b");
 * mos.write(key, new Text("value"), generateFileName(key, new Text("value")));
 * ...
 * }
 * 
 * public void cleanup(Context) throws IOException {
 * mos.close();
 * ...
 * }
 * 
 * }
 * </pre>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class AvroMultipleOutputs<K, V>{

	private static final String							MULTIPLE_OUTPUTS	= "mapreduce.multipleoutputs";

	private static final String							MO_PREFIX					= "mapreduce.multipleoutputs.namedOutput.";

	private static final String							FORMAT						= ".format";
	private static final String							KEY								= ".key";
	private static final String							VALUE							= ".value";
	private static final String							COUNTERS_ENABLED	= "mapreduce.multipleoutputs.counters";

	/**
	 * Counters group used by the counters of MultipleOutputs.
	 */
	private static final String							COUNTERS_GROUP		= AvroMultipleOutputs.class
																																.getName();

	/**
	 * Cache for the taskContexts
	 */
	private Map<String, TaskAttemptContext>	taskContexts			= new HashMap<String, TaskAttemptContext>();

	/**
	 * Checks if a named output name is valid token.
	 * 
	 * @param namedOutput
	 *          named output Name
	 * @throws IllegalArgumentException
	 *           if the output name is not valid.
	 */
	private static void checkTokenName(String namedOutput) {
		if (namedOutput == null || namedOutput.length() == 0) {
			throw new IllegalArgumentException("Name cannot be NULL or emtpy");
		}
		for (char ch : namedOutput.toCharArray()) {
			if ((ch >= 'A') && (ch <= 'Z')) {
				continue;
			}
			if ((ch >= 'a') && (ch <= 'z')) {
				continue;
			}
			if ((ch >= '0') && (ch <= '9')) {
				continue;
			}
			throw new IllegalArgumentException("Name cannot be have a '" + ch
					+ "' char");
		}
	}

		/**
	 * Checks if a named output name is valid.
	 * 
	 * @param namedOutput
	 *          named output Name
	 * @throws IllegalArgumentException
	 *           if the output name is not valid.
	 */
	private void checkNamedOutputName(String namedOutput,
			boolean alreadyDefined) {
		checkTokenName(namedOutput);
		if (alreadyDefined && namedOutputs.containsKey(namedOutput)) {
			throw new IllegalArgumentException("Named output '" + namedOutput
					+ "' already alreadyDefined");
		} else if (!alreadyDefined && !namedOutputs.containsKey(namedOutput)) {
			throw new IllegalArgumentException("Named output '" + namedOutput
					+ "' not defined");
		}
	}

//	// Returns list of channel names.
//	private static List<String> getNamedOutputsList(JobContext job) {
//		List<String> names = new ArrayList<String>();
//		StringTokenizer st = new StringTokenizer(job.getConfiguration().get(
//				MULTIPLE_OUTPUTS, ""), " ");
//		while (st.hasMoreTokens()) {
//			names.add(st.nextToken());
//		}
//		return names;
//	}

//	// Returns the named output OutputFormat.
//	@SuppressWarnings("unchecked")
//	private static Class<? extends OutputFormat<?, ?>> getNamedOutputFormatClass(
//			JobContext job, String namedOutput) {
//		return (Class<? extends OutputFormat<?, ?>>) job.getConfiguration()
//				.getClass(MO_PREFIX + namedOutput + FORMAT, null, OutputFormat.class);
//	}
//
//	// Returns the key class for a named output.
//	private static Class<?> getNamedOutputKeyClass(JobContext job,
//			String namedOutput) {
//		return job.getConfiguration().getClass(MO_PREFIX + namedOutput + KEY, null);
//	}
//
//	// Returns the value class for a named output.
//	private static Class<?> getNamedOutputValueClass(JobContext job,
//			String namedOutput) {
//
//		return job.getConfiguration().getClass(MO_PREFIX + namedOutput + VALUE,
//				null);
//
//	}

	/**
	 * Adds a named output for the job.
	 * <p/>
	 * 
	 * @param namedOutput
	 *          named output name, it has to be a word, letters and numbers only,
	 *          cannot be the word 'part' as that is reserved for the default
	 *          output.
	 * @param outputFormatClass
	 *          OutputFormat class.
	 * @param keyClass
	 *          key class
	 * @param valueClass
	 *          value class
	 */
	public void addNamedOutput(String namedOutput,
			Class<? extends OutputFormat> outputFormatClass, Class<?> keyClass,
			Class<?> valueClass) {
		checkNamedOutputName(namedOutput, true);
		
		namedOutputs.put(namedOutput, new NameOutput(outputFormatClass,keyClass,valueClass));
		
	}

	// instance code, to be used from Mapper/Reducer code
	private class NameOutput
	{
		public NameOutput(Class<? extends OutputFormat> outputFormatClass,Class<?> keyClass,Class<?> valueClass)
		{
			this.outputFormatClass = outputFormatClass;
			this.keyClass = keyClass;
			this.valueClass = valueClass;
		}
		Class<? extends OutputFormat> outputFormatClass; 
		Class<?> keyClass;
		Class<?> valueClass;
	}
	private TaskAttemptContext	context;
	private Map<String,NameOutput>																			namedOutputs;
	
	private Map<String, RecordWriter<?, ?>>									recordWriters;

	/**
	 * Creates and initializes multiple outputs support, it should be instantiated
	 * in the Mapper/Reducer setup method.
	 * 
	 * @param context
	 *          the TaskInputOutputContext object
	 */
	public AvroMultipleOutputs(
			TaskAttemptContext context) {
		this.context = context;
		namedOutputs = new HashMap<String, NameOutput>();
		recordWriters = new HashMap<String, RecordWriter<?, ?>>();
	}

	/**
	 * Write key and value to the namedOutput.
	 * 
	 * Output path is a unique file generated for the namedOutput. For example,
	 * {namedOutput}-(m|r)-{part-number}
	 * 
	 * @param namedOutput
	 *          the named output name
	 * @param key
	 *          the key
	 * @param value
	 *          the value
//	 */
	@SuppressWarnings("unchecked")
	public <K, V> void write(String namedOutput, K key, V value)
			throws IOException, InterruptedException {
		write(namedOutput, key, value, namedOutput);
	}

	/**
	 * Write key and value to baseOutputPath using the namedOutput.
	 * 
	 * @param namedOutput
	 *          the named output name
	 * @param key
	 *          the key
	 * @param value
	 *          the value
	 * @param baseOutputPath
	 *          base-output path to write the record to. Note: Framework will
	 *          generate unique filename for the baseOutputPath
	 */
	@SuppressWarnings("unchecked")
	public <K, V> void write(String namedOutput, K key, V value,
			String baseOutputPath) throws IOException, InterruptedException {
		checkNamedOutputName(namedOutput, false);
		if (!namedOutputs.containsKey(namedOutput)) {
			throw new IllegalArgumentException("Undefined named output '"
					+ namedOutput + "'");
		}
		TaskAttemptContext taskContext = getContext(namedOutput);
		getRecordWriter(taskContext, baseOutputPath).write(key, value);
	}

	// by being synchronized MultipleOutputTask can be use with a
	// MultithreadedMapper.
	@SuppressWarnings("unchecked")
	private synchronized RecordWriter getRecordWriter(
			TaskAttemptContext taskContext, String baseFileName) throws IOException,
			InterruptedException {

		// look for record-writer in the cache
		RecordWriter writer = recordWriters.get(baseFileName);

		// If not in cache, create a new one
		if (writer == null) {
			// get the record writer from context output format
			FileOutputFormat.setOutputName(taskContext, baseFileName);
			try {
				writer = ((OutputFormat) ReflectionUtils.newInstance(
						taskContext.getOutputFormatClass(), taskContext.getConfiguration()))
						.getRecordWriter(taskContext);
			} catch (ClassNotFoundException e) {
				throw new IOException(e);
			}
			
			// add the record-writer to the cache
			recordWriters.put(baseFileName, writer);
		}
		return writer;
	}

	// Create a taskAttemptContext for the named output with
	// output format and output key/value types put in the context
	private TaskAttemptContext getContext(String nameOutput) throws IOException {

		TaskAttemptContext taskContext = taskContexts.get(nameOutput);

		if (taskContext != null) {
			return taskContext;
		}

		// The following trick leverages the instantiation of a record writer via
		// the job thus supporting arbitrary output formats.
		NameOutput out = namedOutputs.get(nameOutput);
		Job job = new Job(context.getConfiguration());
		job.setOutputFormatClass(out.outputFormatClass);
		job.setOutputKeyClass(out.keyClass);
		job.setOutputValueClass(out.valueClass);
		taskContext = new TaskAttemptContextImpl(job.getConfiguration(),
				context.getTaskAttemptID());

		taskContexts.put(nameOutput, taskContext);

		return taskContext;
	}

	/**
	 * Closes all the opened outputs.
	 * 
	 * This should be called from cleanup method of map/reduce task. If overridden
	 * subclasses must invoke <code>super.close()</code> at the end of their
	 * <code>close()</code>
	 * 
	 */
	@SuppressWarnings("unchecked")
	public void close() throws IOException, InterruptedException {
		for (RecordWriter writer : recordWriters.values()) {
			writer.close(context);
		}
	}
}

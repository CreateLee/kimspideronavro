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

package kim.spider.io;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

import kim.spider.io.HashAvroFile.Writer;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.mapred.Pair;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.Progressable;

/**
 * A file-based map from keys to values.
 * 
 * <p>
 * A map is a directory containing two files, the <code>data</code> file,
 * containing all keys and values in the map, and a smaller <code>index</code>
 * file, containing a fraction of the keys. The fraction is determined by
 * {@link Writer#getIndexInterval()}.
 * 
 * <p>
 * The index file is read entirely into memory. Thus key implementations should
 * try to keep themselves small.
 * 
 * <p>
 * Map files are created by adding entries in-order. To maintain a large
 * database, perform updates by copying the previous version of a database and
 * merging in a sorted change list, to create a new version of the database in a
 * new file. Sorting large change lists can be done with
 * {@link SequenceFile.Sorter}.
 */
public class HashAvroFile {
//	private static final Log		LOG							= LogFactory
//																									.getLog(HashAvroFile.class);

	/** The name of the index file. */
	public static final String	INDEX_FILE_NAME	= "index.avro";

	/** The name of the data file. */
	public static final String	DATA_FILE_NAME	= "data.avro";

	protected HashAvroFile() {
	} // no public ctor

	/** Writes a new map. */
	public static class Writer<K, V> implements java.io.Closeable {
		private DataFileWriter<Pair<K, V>>		data;
		private DataFileWriter<Pair<K, Long>>	index;
		/** The configuration key for Avro deflate level. */
		public static final String						DEFLATE_LEVEL_KEY			= "avro.mapred.deflate.level";

		/** The default deflate level. */
		public static final int								DEFAULT_DEFLATE_LEVEL	= 1;

		private long													size;
		// private LongWritable position = new LongWritable();

		// the following fields are used only for checking key order
		private KeyComparator<K>					comparator;
		private Schema												keySchema;
		private Schema												valueSchema;

		/**
		 * What was size when we last wrote an index. Set to MIN_VALUE to ensure
		 * that we have an index at position zero -- midKey will throw an exception
		 * if this is not the case
		 */

		public Writer(Configuration conf, FileSystem fs, String dirName,
				Schema keySchema, Schema valueSchema) throws IOException {
			this(conf, fs, dirName, keySchema, valueSchema, null,
					DEFAULT_DEFLATE_LEVEL);
		}

		public Writer(Configuration conf, FileSystem fs, String dirName,
				Schema keySchema, Schema valueSchema, Progressable progress)
				throws IOException {
			this(conf, fs, dirName, keySchema, valueSchema, progress,
					DEFAULT_DEFLATE_LEVEL);
		}

		public Writer(Configuration conf, FileSystem fs, String dirName,
				Schema keySchema, Schema valueSchema, Progressable progress,
				int deflateLevel) throws IOException {
			this.keySchema = keySchema;
			this.valueSchema = valueSchema;
			comparator = new KeyComparator<K>();
			comparator.setSchema(keySchema);
			Path dir = new Path(dirName);
			if (!fs.mkdirs(dir)) {
				throw new IOException("Mkdirs failed to create directory "
						+ dir.toString());
			}
			Path dataFile = new Path(dir, DATA_FILE_NAME);
			Path indexFile = new Path(dir, INDEX_FILE_NAME);

			data = new DataFileWriter<Pair<K, V>>(
					new SpecificDatumWriter<Pair<K, V>>());

			data.setCodec(CodecFactory.deflateCodec(deflateLevel));
			data.create(new Pair<K, V>(Pair.getPairSchema(keySchema, valueSchema))
					.getSchema(), dataFile.getFileSystem(conf).create(dataFile, progress));

			index = new DataFileWriter<Pair<K, Long>>(
					new SpecificDatumWriter<Pair<K, Long>>());

			index.setCodec(CodecFactory.deflateCodec(deflateLevel));

			index.create(
					new Pair<K, V>(
							Pair.getPairSchema(keySchema, Schema.create(Type.LONG)))
							.getSchema(),
					indexFile.getFileSystem(conf).create(indexFile, progress));

		}

		/** Close the map. */
		public synchronized void close() throws IOException {
			data.close();
			index.close();
		}

		/**
		 * Append a key/value pair to the map. The key must be greater or equal to
		 * the previous key added to the map.
		 */
		public synchronized void append(K key, V val) throws IOException {

			long pos = data.sync();
			// Only write an index if we've changed positions. In a block compressed
			// file, this means we write an entry at the start of each block
			// position.set(pos); // point to current eof
			index.append(new Pair<K, Long>(key, keySchema, pos, Schema
					.create(Type.LONG)));

			data.append(new Pair<K, V>(key, keySchema, val, valueSchema)); // append
																																			// key/value
																																			// to data
			size++;
		}

	}

	/** Provide access to an existing map. */
	public static class Reader<K, V> implements java.io.Closeable {

		private KeyComparator<K>					comparator;
		
		private long													firstPosition;
		private K															finalKey		= null;
		private K															firstKey		= null;
		// the data, on disk
		private DataFileReader<Pair<K, V>>		data;
		private DataFileReader<Pair<K, Long>>	index;

//		private Schema												keySchema;
//		private Schema												valueSchema;

		// whether the index Reader was closed
		private boolean												indexClosed	= false;

		private HashMap<K, Long>							keys;

		/** Construct a map reader for the named map. */
		public Reader(FileSystem fs, String dirName, Configuration conf) throws IOException {
//			this.keySchema = keySchema;
//			this.valueSchema = valueSchema;
			open(fs, dirName, conf);
		}

		protected synchronized void open(FileSystem fs, String dirName,
				Configuration conf) throws IOException {
			Path dir = new Path(dirName);
			Path dataFile = new Path(dir, DATA_FILE_NAME);
			Path indexFile = new Path(dir, INDEX_FILE_NAME);

			// open the data
//			this.data = new DataFileReader<Pair<K, V>>(new FsInput(dataFile, conf),
//					new SpecificDatumReader<Pair<K, V>>(Pair.getPairSchema(keySchema,
//							valueSchema)));
			this.data = new DataFileReader<Pair<K, V>>(new FsInput(dataFile, conf),
					new SpecificDatumReader<Pair<K, V>>());
			this.firstPosition = data.previousSync();

			comparator = new KeyComparator<K>();
			comparator.setSchema(Pair.getKeySchema(data.getSchema()));

			// open the index
//			this.index = new DataFileReader<Pair<K, Long>>(new FsInput(indexFile,
//					conf), new SpecificDatumReader<Pair<K, Long>>(Pair.getPairSchema(
//					keySchema, Schema.create(Type.LONG))));
			this.index = new DataFileReader<Pair<K, Long>>(new FsInput(indexFile,
					conf), new SpecificDatumReader<Pair<K, Long>>());
		}

		private int	count	= 0;
		
		public Schema getKeySchema()
		{
			return Pair.getKeySchema(data.getSchema());
		}
		
		public Schema getValueSchema()
		{
			return Pair.getValueSchema(data.getSchema());
		}
		
		private void readIndex() throws IOException {
			// read the index entirely into memory
			if (this.keys != null)
				return;
			keys = new HashMap<K, Long>(1024);
			try {
				while (index.hasNext()) {
					Pair<K, Long> pair;// = new Pair<K, Long>(index.getSchema());
					pair = index.next();
					keys.put(pair.key(), pair.value());
					if (firstKey == null)
						firstKey = pair.key();
					finalKey = pair.key();
					count++;
				}

			}finally {
				indexClosed = true;
				index.close();
			}
		}

		/** Re-positions the reader before its first key. */
		public synchronized void reset() throws IOException {
			data.seek(firstPosition);
		}

		/**
		 * number block in this file.
		 * 
		 * @param key
		 *          key to read into
		 */

		public synchronized int size() throws IOException {
			readIndex();
			return keys.size();
		}

		/**
		 * Reads the final key from the file.
		 * 
		 * @param key
		 *          key to read into
		 */
		public synchronized K finalKey() throws IOException {
			readIndex();
			return finalKey;
		}

		/**
		 * Positions the reader at the named key, or if none such exists, at the key
		 * that falls just before or just after dependent on how the
		 * <code>before</code> parameter is set.
		 * 
		 * @param before
		 *          - IF true, and <code>key</code> does not exist, position file at
		 *          entry that falls just before <code>key</code>. Otherwise,
		 *          position file at record that sorts just after.
		 * @return 0 - exact match found < 0 - positioned at next record 1 - no more
		 *         records in file
		 */
		
		private synchronized Pair<K, V> seekInternal(K key) throws IOException {
			readIndex(); // make sure index is read

			Long pos = keys.get(key);
			if (pos != null) {
				data.seek(pos);
				return data.next();
			} else
				return null;
		}

		public synchronized boolean hasNext() throws IOException {
			return data.hasNext();
		}

		/**
		 * Read the next key/value pair in the map into <code>key</code> and
		 * <code>val</code>. Returns true if such a pair exists and false when at
		 * the end of the map
		 */
		public synchronized Pair<K, V> next() throws IOException {
			return data.next();
		}

		/** Return the value for the named key, or null if none exists. */
		public synchronized V get(K key) throws IOException {
			readIndex();
			
			Pair<K, V> pair = seekInternal(key);
			if (pair != null) {
				return pair.value();
			} else
				return null;
		}

		/** Close the map. */
		public synchronized void close() throws IOException {
			if (!indexClosed) {
				index.close();
			}
			data.close();
		}

	}

	/** Renames an existing map directory. */
	public static void rename(FileSystem fs, String oldName, String newName)
			throws IOException {
		Path oldDir = new Path(oldName);
		Path newDir = new Path(newName);
		if (!fs.rename(oldDir, newDir)) {
			throw new IOException("Could not rename " + oldDir + " to " + newDir);
		}
	}

	/** Deletes the named map file. */
	public static void delete(FileSystem fs, String name) throws IOException {
		Path dir = new Path(name);
		Path data = new Path(dir, DATA_FILE_NAME);
		Path index = new Path(dir, INDEX_FILE_NAME);

		fs.delete(data, true);
		fs.delete(index, true);
		fs.delete(dir, true);
	}

	/**
	 * This method attempts to fix a corrupt MapFile by re-creating its index.
	 * 
	 * @param <K>
	 * 
	 * @param fs
	 *          filesystem
	 * @param dir
	 *          directory containing the MapFile data and index
	 * @param keyClass
	 *          key class (has to be a subclass of Writable)
	 * @param valueClass
	 *          value class (has to be a subclass of Writable)
	 * @param dryrun
	 *          do not perform any changes, just report what needs to be done
	 * @return number of valid entries in this MapFile, or -1 if no fixing was
	 *         needed
	 * @throws Exception
	 */
	public static <K, V> long fix(FileSystem fs, Path dir, Schema keySchema,
			Schema valueSchema, boolean dryrun, Configuration conf) throws Exception {
		String dr = (dryrun ? "[DRY RUN ] " : "");
		Path data = new Path(dir, DATA_FILE_NAME);
		Path index = new Path(dir, INDEX_FILE_NAME);
		if (!fs.exists(data)) {
			// there's nothing we can do to fix this!
			throw new Exception(dr + "Missing data file in " + dir
					+ ", impossible to fix this.");
		}
		if (fs.exists(index)) {
			// no fixing needed
			return -1;
		}
		DataFileReader<Pair<K, V>> dataReader = new DataFileReader<Pair<K, V>>(
				new FsInput(data, conf), new SpecificDatumReader<Pair<K, V>>(
						Schema.create(Type.NULL)));
		;

		long cnt = 0L;
		DataFileWriter<Pair<K, Long>> indexWriter = null;
		if (!dryrun) {
			indexWriter = new DataFileWriter<Pair<K, Long>>(
					new SpecificDatumWriter<Pair<K, Long>>());

			int level = conf.getInt(AvroOutputFormat.DEFLATE_LEVEL_KEY,
					HashAvroFile.Writer.DEFAULT_DEFLATE_LEVEL);
			indexWriter.setCodec(CodecFactory.deflateCodec(level));

			// copy metadata from job
			for (Map.Entry<String, String> e : conf) {
				if (e.getKey().startsWith(AvroJob.TEXT_PREFIX))
					indexWriter.setMeta(e.getKey()
							.substring(AvroJob.TEXT_PREFIX.length()), e.getValue());
				if (e.getKey().startsWith(AvroJob.BINARY_PREFIX))
					indexWriter.setMeta(
							e.getKey().substring(AvroJob.BINARY_PREFIX.length()), URLDecoder
									.decode(e.getValue(), "ISO-8859-1").getBytes("ISO-8859-1"));
			}

			indexWriter.create(
					new Pair<K, V>(
							Pair.getPairSchema(keySchema, Schema.create(Type.LONG)))
							.getSchema(), index.getFileSystem(conf).create(index));
		}
		try {
			long pos = 0L;
			while (dataReader.hasNext()) {
				Pair<K, V> pair = dataReader.next();
				cnt++;
				pos = dataReader.previousSync();
				if (!dryrun)
					indexWriter.append(new Pair<K, Long>(pair.key(), keySchema, pos,
							Schema.create(Type.LONG)));
			}
		} catch (Throwable t) {
			// truncated data file. swallow it.
		}
		dataReader.close();
		if (!dryrun)
			indexWriter.close();
		return cnt;
	}

	public static void main(String[] args) throws Exception {
		String in = "in";

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.getLocal(conf);
		Schema keySchema = Schema.create(Schema.Type.STRING);
		Schema valueSchema = Schema.parse(fs.open(new Path("Datum.avsc")));
		long start = System.currentTimeMillis();
		// HashAvroFile.Writer writer = new HashAvroFile.Writer(conf, fs, in,
		// keySchema, valueSchema, null);
		// for (int i = 0; i < 1000000; i++) {
		// GenericData.Record sd = new GenericData.Record(valueSchema);
		// sd.put("status", "STATUS_DB_GONE");
		// sd.put("fetchTime", System.currentTimeMillis());
		// sd.put("fetchInterval", 30);
		// sd.put("score", new Float(i));
		// HashMap<Utf8, Utf8> hm = new HashMap<Utf8, Utf8>();
		// hm.put(new Utf8("parse_class"), new Utf8("defaultclass"));
		// sd.put("extend", hm);
		// HashMap<Utf8, Utf8> hm1 = new HashMap<Utf8, Utf8>();
		// hm1.put(new Utf8("metaData"), new Utf8("mdata"));
		// sd.put("metaData", hm1);
		// writer.append(new Utf8("url"+i), sd);
		// }
		// writer.close();
		// System.out.println((System.currentTimeMillis() - start));
		HashAvroFile.Reader reader = new HashAvroFile.Reader(fs, in,
				conf);
		start = System.currentTimeMillis();
		for (int i = 0; i < 10000; i++) {
			// String key = "url" + i;
			// reader.get("url"+(int) (Math.random() * 1000000));
			Utf8 key = new Utf8("url" + (int) (Math.random() * 1000000));

			System.out.println(key + reader.get(key).toString());
		}
		System.out.println((System.currentTimeMillis() - start));

		start = System.currentTimeMillis();
		reader.reset();
		while (reader.hasNext()) {
			reader.next().toString();
		}
		System.out.println((System.currentTimeMillis() - start));
		System.out.println(reader.size());
		System.out.println(reader.finalKey().toString());

	}

}

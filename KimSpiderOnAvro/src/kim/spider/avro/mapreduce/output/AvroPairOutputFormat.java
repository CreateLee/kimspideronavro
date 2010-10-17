package kim.spider.avro.mapreduce.output;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.mapred.Pair;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AvroPairOutputFormat<K, V>
		extends FileOutputFormat<K, V> {

	/** The file name extension for avro data files. */
	public final static String	EXT										= ".avro";

	/** The configuration key for Avro deflate level. */
	public static final String	DEFLATE_LEVEL_KEY			= "avro.mapred.deflate.level";

	/** The default deflate level. */
	public static final int			DEFAULT_DEFLATE_LEVEL	= 1;

	/** Enable output compression using the deflate codec and specify its level. */
	public static void setDeflateLevel(Job job, int level) {
		FileOutputFormat.setCompressOutput(job, true);
		job.getConfiguration().setInt(DEFLATE_LEVEL_KEY, level);
	}

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		final DataFileWriter<Pair<K, V>> writer = new DataFileWriter<Pair<K, V>>(
				new ReflectDatumWriter<Pair<K, V>>());

		if (FileOutputFormat.getCompressOutput(job)) {
			int level = job.getConfiguration().getInt(DEFLATE_LEVEL_KEY,
					DEFAULT_DEFLATE_LEVEL);
			writer.setCodec(CodecFactory.deflateCodec(level));
		}
		Path path = getDefaultWorkFile(job, EXT);
		final Schema keySchema = ReflectData.get().getSchema(job.getOutputKeyClass());
		final Schema valueSchema = ReflectData.get()
				.getSchema(job.getOutputValueClass());
		writer.create(Pair.getPairSchema(keySchema, valueSchema), path
				.getFileSystem(job.getConfiguration()).create(path));

		return new RecordWriter<K, V>() {

			@Override
			public void write(K key, V value) throws IOException {

				// TODO Auto-generated method stub
				writer.append(new Pair<K, V>(key,keySchema, value,valueSchema));
			}

			@Override
			public void close(TaskAttemptContext context) throws IOException,
					InterruptedException {
				// TODO Auto-generated method stub
				writer.close();
			}
		};
	}

}
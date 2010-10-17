package kim.spider.crawl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import kim.spider.io.SpiderData;

import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

public class KimSolution {

	public static class Mappers1 extends Mapper<Text, Text, Text, Text> {

		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {

			context.write(key, value);
			context.write(value, key);
		}
	}

	public static class Reducers1 extends
			Reducer<Text, Text, Text, sortedStringArr> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			ArrayList<String> arrs = new ArrayList<String>();
			while (values.iterator().hasNext()) {
				arrs.add(values.iterator().next().toString());
			}
			if (arrs.size() > 0) {
				context.write(
						key,
						new sortedStringArr(
								arrs.toArray(new String[arrs.size()])));
			}
		}
	}

	public static class Mappers2 extends
			Mapper<Text, sortedStringArr, triangleDot, triangleLine> {

		public void map(Text key, sortedStringArr value, Context context)
				throws IOException, InterruptedException {

			/*
			 * <Text, sortedStringArr>描述了点a出发的所有线，其中每两条线构成一个不完整的三角形
			 */
			if (value.dotarr != null) {
				// 找出从A点出发的任意两条线的组合
				for (int i = 0; i < value.dotarr.length; i++) {
					for (int k = i + 1; k < value.dotarr.length; k++) {
						triangleDot tdot = new triangleDot(key.toString(),
								value.dotarr[i], value.dotarr[k]);
						triangleLine tline = new triangleLine();
						tline.addLine(new Line(key.toString(), value.dotarr[i]));
						tline.addLine(new Line(key.toString(), value.dotarr[k]));
						context.write(tdot, tline);
					}
				}
			}
		}
	}

	public static class Reducers2 extends
			Reducer<triangleDot, triangleLine, triangleDot, triangleLine> {

		public void reduce(triangleDot key, Iterable<triangleLine> values,
				Context context) throws IOException, InterruptedException {

			triangleLine tl = new triangleLine();
			while (values.iterator().hasNext()) {
				//合并相同点构成三角形的所有线
				tl.combine(values.iterator().next());
			}
			if(tl.isTriangle())
				context.write(key, tl);
		}
	}
	/*
	 * 一个排序的点的数组，实现了Writable接口
	 */
	public static class sortedStringArr implements Writable {
		public String[] dotarr;

		public sortedStringArr() {

		}

		public sortedStringArr(String[] arr) {
			this.dotarr = arr;
			Arrays.sort(dotarr);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			if (dotarr == null)
				out.writeInt(0);
			else {
				out.writeInt(dotarr.length);
				for (String s : dotarr) {
					if (s != null) {
						out.writeBoolean(true);
						Text.writeString(out, s);
					} else
						out.writeBoolean(false);
				}
			}

		}

		@Override
		public void readFields(DataInput in) throws IOException {
			dotarr = null;
			int ilen = in.readInt();
			if (ilen != 0) {
				dotarr = new String[ilen];
				for (int i = 0; i < ilen; i++) {
					if (in.readBoolean())
						dotarr[i] = Text.readString(in);
				}
			}
			Arrays.sort(dotarr);
		}
	}

	public static class triangleDot implements WritableComparable<triangleDot> {
		public String[] dotarr = new String[3];

		public triangleDot() {

		}

		public triangleDot(String dota, String dotb, String dotc) {
			dotarr[0] = dota;
			dotarr[1] = dotb;
			dotarr[2] = dotc;
			Arrays.sort(dotarr);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			for (String s : dotarr) {
				if (s != null) {
					out.writeBoolean(true);
					Text.writeString(out, s);
				} else
					out.writeBoolean(false);
			}

		}

		@Override
		public void readFields(DataInput in) throws IOException {
			dotarr = new String[3];
			for (int i=0;i<3;i++) {
				if (in.readBoolean())
					dotarr[i] = Text.readString(in);
				else 
					dotarr[i] = "";
			}
			Arrays.sort(dotarr);
		}

		@Override
		public int compareTo(triangleDot o) {
			Arrays.sort(o.dotarr);
			Arrays.sort(dotarr);
			if (!dotarr[0].equals(o.dotarr[0]))
				return dotarr[0].compareTo(o.dotarr[0]);
			else if (!dotarr[1].equals(o.dotarr[1]))
				return dotarr[1].compareTo(o.dotarr[1]);
			else
				return dotarr[2].compareTo(o.dotarr[2]);
		}
		
		public String toString()
		{
			Arrays.sort(dotarr);
			return "triangle with:"+dotarr[0]+","+dotarr[1]+","+dotarr[2];
		}
	}

	/*
	 * 一个用线描述的三角形，用于判断三角形的完整
	 */
	public static class triangleLine implements Writable {
		ArrayList<Line> arrLine = new ArrayList<Line>();

		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			if (arrLine == null)
				out.writeInt(0);
			else {
				out.writeInt(arrLine.size());
				for (Line l : arrLine) {
					l.write(out);
				}
			}
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			arrLine.clear();
			int linenum = in.readInt();
			for (int i = 0; i < linenum; i++) {
				Line l = new Line();
				l.readFields(in);
				arrLine.add(l);
			}
		}

		public void addLine(Line add) {
			if (!arrLine.contains(add))
				arrLine.add(add);
		}

		public void combine(triangleLine b) {
			if (b.arrLine != null) {
				for (Line l : b.arrLine) {
					this.addLine(l);
				}
			}
		}
		public boolean isTriangle()
		{
			return arrLine.size()>=3?true:false;
		}
		
	}

	/*
	 * 定义一条有向线，小->大
	 */
	public static class Line implements WritableComparable<Line> {
		public String dotMin = "";
		public String dotMax = "";

		public Line(String a, String b) {
			if (a.compareTo(b) > 0) {
				dotMax = a;
				dotMin = b;
			} else {
				dotMin = a;
				dotMax = b;
			}

		}

		public Line() {

		}

		@Override
		public void write(DataOutput out) throws IOException {
			Text.writeString(out, dotMin);
			Text.writeString(out, dotMax);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			dotMin = Text.readString(in);
			dotMax = Text.readString(in);
		}

		@Override
		public int compareTo(Line o) {
			if (!dotMin.equals(o.dotMin)) {
				return dotMin.compareTo(o.dotMin);
			} else
				return dotMax.compareTo(o.dotMax);
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		// String[] otherArgs = new GenericOptionsParser(conf,
		// args).getRemainingArgs();
		System.out.print(ReflectData.get().getSchema(SpiderData.class));
		
		FileSystem fs = FileSystem.getLocal(conf);
		Writer writer = SequenceFile.createWriter(fs, conf,
				new Path("infile").makeQualified(fs), Text.class, Text.class);

		writer.append(new Text("A"), new Text("B"));
		writer.append(new Text("A"), new Text("C"));
		writer.append(new Text("A"), new Text("D"));
		writer.append(new Text("A"), new Text("E"));

		writer.append(new Text("B"), new Text("C"));
		writer.append(new Text("B"), new Text("F"));
		writer.append(new Text("C"), new Text("G"));
		writer.append(new Text("E"), new Text("D"));

		writer.append(new Text("W"), new Text("D"));
		writer.append(new Text("W"), new Text("E"));
		
		writer.append(new Text("asf"), new Text("fs"));
		writer.append(new Text("fs"), new Text("qe"));
		writer.append(new Text("asf"), new Text("qe"));
		
		writer.close();

		Job job = new Job(conf, "find line");
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapperClass(Mappers1.class);
		job.setReducerClass(Reducers1.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(sortedStringArr.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path("infile"));
		FileOutputFormat.setOutputPath(job, new Path("lineTemp"));

		Job job2 = new Job(conf, "find triangle");
		job2.setInputFormatClass(SequenceFileInputFormat.class);
		job2.setMapperClass(Mappers2.class);
		// job2.setCombinerClass(Reducers2.class);
		job2.setReducerClass(Reducers2.class);
		job2.setOutputKeyClass(triangleDot.class);
		job2.setOutputValueClass(triangleLine.class);
		job2.setOutputFormatClass(new FileOutputFormat<triangleDot, triangleLine>() {
			class LineRecordWriter<K, V> extends RecordWriter<K, V> {
				private static final String utf8 = "UTF-8";
				protected DataOutputStream out;

				// private final byte[] keyValueSeparator;

				public LineRecordWriter(DataOutputStream out) {
					this.out = out;

				}

				public synchronized void write(K key, V value)
						throws IOException {

					out.write((key.toString()+"\n").getBytes());
				}

				public synchronized void close(TaskAttemptContext context)
						throws IOException {
					out.close();
				}
			}

			@Override
			public RecordWriter<triangleDot, triangleLine> getRecordWriter(
					TaskAttemptContext job) throws IOException,
					InterruptedException {
				Configuration conf = job.getConfiguration();
				boolean isCompressed = getCompressOutput(job);
				CompressionCodec codec = null;
				String extension = "";
				if (isCompressed) {
					Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(
							job, GzipCodec.class);
					codec = (CompressionCodec) ReflectionUtils.newInstance(
							codecClass, conf);
					extension = codec.getDefaultExtension();
				}
				Path file = getDefaultWorkFile(job, extension);
				FileSystem fs = file.getFileSystem(conf);
				if (!isCompressed) {
					FSDataOutputStream fileOut = fs.create(file, false);
					return new LineRecordWriter<triangleDot, triangleLine>(fileOut);
				} else {
					FSDataOutputStream fileOut = fs.create(file, false);
					return new LineRecordWriter<triangleDot, triangleLine>(
							new DataOutputStream(codec
									.createOutputStream(fileOut)));
				}
			}
		}.getClass());
		FileInputFormat.addInputPath(job2, new Path("lineTemp"));

		FileOutputFormat.setOutputPath(job2, new Path("triangleout"));

		Path out1 = new Path("lineTemp");
		if (fs.exists(out1))
			fs.delete(out1, true);

		Path out2 = new Path("triangleout");
		if (fs.exists(out2))
			fs.delete(out2, true);

		if (job.waitForCompletion(true)) {
			System.exit(job2.waitForCompletion(true) ? 0 : 1);
		}

	}
}

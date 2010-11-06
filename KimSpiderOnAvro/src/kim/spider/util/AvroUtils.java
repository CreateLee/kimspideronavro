package kim.spider.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.ipc.ByteBufferInputStream;
import org.apache.avro.ipc.ByteBufferOutputStream;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;

public class AvroUtils {
	public static <V> Object clone(V v) throws IOException {
		ByteBufferOutputStream out = new ByteBufferOutputStream();
		BinaryEncoder encoder = new BinaryEncoder(out);
		Schema schema;
		if (IndexedRecord.class.isAssignableFrom(v.getClass()))
			schema = ((IndexedRecord) v).getSchema();
		else
			schema = ReflectData.get().getSchema(v.getClass());

		ReflectDatumWriter<V> writer = new ReflectDatumWriter<V>(schema);
		ReflectDatumReader<V> reader = new ReflectDatumReader<V>(schema);
		writer.write(v, encoder);
		ByteBufferInputStream in = new ByteBufferInputStream(
				out.getBufferList());
		BinaryDecoder decoder = DecoderFactory.defaultFactory()
				.createBinaryDecoder(in, null);
		return reader.read(null, decoder);

	}

	public static <V> String toJsonString(V v) {
		StringBuilder buffer = new StringBuilder();
		Schema schema;
		if (IndexedRecord.class.isAssignableFrom(v.getClass()))
			schema = ((IndexedRecord) v).getSchema();
		else
			schema = ReflectData.get().getSchema(v.getClass());
		toString(v,buffer,schema);

		return buffer.toString();
	}
	
	protected static void toString(Object datum, StringBuilder buffer,
			Schema schema) {
		if(datum == null)
		{
			buffer.append("{null}");
			return;
		}
		if (schema.getType() == Schema.Type.RECORD) {
			buffer.append("{");
			int count = 0;

			for (Field f : schema.getFields()) {
				toString(f.name(), buffer, Schema.create(Schema.Type.STRING));
				buffer.append(": ");
				toString(ReflectData.get().getField(datum, f.name(), count),
						buffer, f.schema());
				// toString(record.get(f.pos()), buffer);
				if (++count < schema.getFields().size())
					buffer.append(", ");
			}
			buffer.append("}");
		} else if (schema.getType() == Schema.Type.ARRAY) {

			if (datum instanceof Collection) {
				Collection<?> array = (Collection<?>) datum;
				buffer.append("[");
				long last = array.size() - 1;
				int i = 0;
				for (Object element : array) {
					toString(element, buffer, schema.getElementType());
					if (i++ < last)
						buffer.append(", ");
				}
				buffer.append("]");
			} else if (datum != null && datum.getClass().isArray()) {
				buffer.append("[");
				long last = ((Object[]) datum).length - 1;
				int i = 0;
				for (Object element : ((Object[]) datum)) {
					toString(element, buffer, schema.getElementType());
					if (i++ < last)
						buffer.append(", ");
				}
				buffer.append("]");
			}
		} else if (schema.getType() == Schema.Type.MAP) {
			buffer.append("{");
			int count = 0;
			@SuppressWarnings(value = "unchecked")
			Map<Object, Object> map = (Map<Object, Object>) datum;
			for (Map.Entry<Object, Object> entry : map.entrySet()) {
				toString(entry.getKey(), buffer, schema.getValueType());
				buffer.append(": ");
				toString(entry.getValue(), buffer, schema.getValueType());
				if (++count < map.size())
					buffer.append(", ");
			}
			buffer.append("}");
		} else if (schema.getType() == Schema.Type.STRING) {
			buffer.append("\"");
			buffer.append(datum); // TODO: properly escape!
			buffer.append("\"");
		} else if (schema.getType() == Schema.Type.FIXED
				|| schema.getType() == Schema.Type.BYTES) {
			if (datum instanceof ByteBuffer) {
				buffer.append("{\"bytes\": \"");
				ByteBuffer bytes = (ByteBuffer) datum;
				for (int i = bytes.position(); i < bytes.limit(); i++)
					buffer.append((char) bytes.get(i));
				buffer.append("\"}");
			}
			else
				buffer.append(datum);
		} else {
			buffer.append(datum);
		}
	}
}

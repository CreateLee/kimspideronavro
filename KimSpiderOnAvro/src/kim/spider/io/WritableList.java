package kim.spider.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;

public class WritableList extends ArrayList<Writable> implements Writable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public static final Log LOG = LogFactory.getLog(WritableList.class);
	public WritableList() {
		super();
	}

	public void readFields(DataInput in) throws IOException {
		this.clear();
		int size = in.readInt();
		for (int i=0;i<size;i++) {
			String cname = Text.readString(in);
			Writable wa;
			try {
				wa = (Writable) Class.forName(cname).newInstance();
				wa.readFields(in);
				this.add(wa);
			} catch (InstantiationException e) {
				LOG.error(cname+":"+StringUtils.stringifyException(e));
			} catch (IllegalAccessException e) {
				LOG.error(cname+":"+StringUtils.stringifyException(e));
			} catch (ClassNotFoundException e) {
				LOG.error(cname+":"+StringUtils.stringifyException(e));
			}
		}
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.size());
		for (Writable wa : this) {
			Text.writeString(out, wa.getClass().getName());
			wa.write(out);
		}
	}

}

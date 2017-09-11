package stubs;

import java.util.Arrays;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

public class DoubleArrayWritable extends ArrayWritable {

	public DoubleArrayWritable(Class<? extends Writable> valueClass, Writable[] values) {
		super(valueClass, values);
	}
	public DoubleArrayWritable(Class<? extends Writable> valueClass) {
		super(valueClass);
	}

	@Override
	public DoubleWritable[] get() {
		return (DoubleWritable[]) super.get();
	}

	@Override
	public String toString() {
		return Arrays.toString(get());
	}

}

package app.job5;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Reducer;

public class OrderRankReducer extends Reducer<DoubleWritable, Text, Text, Text> {

	@Override
	public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		double rank = key.get();
		for (Text value : values) {
			context.write(value, new Text(Double.toString(rank)));
		}
	}

	public static class OrderComparator extends WritableComparator {
		public OrderComparator() {
			super(DoubleWritable.class);
		}

		private Double double1;
		private Double double2;

		@Override
		public int compare(byte[] raw1, int offset1, int length1, byte[] raw2, int offset2, int length2) {
			double1 = ByteBuffer.wrap(raw1, offset1, length1).getDouble();
			double2 = ByteBuffer.wrap(raw2, offset2, length2).getDouble();
			return double2.compareTo(double1);
		}
	}
}

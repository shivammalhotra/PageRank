package app.job2;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OutGraphReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Set<String> unique = new HashSet<>();
		for (Text value : values) {
			if (value.toString().equalsIgnoreCase("!") || value.toString().equals(key.toString()))
				continue;
			unique.add(value.toString());
		}

		String outGraph = "";
		boolean first = true;
		for (String item : unique) {
			if (!first)
				outGraph += "\t";
			outGraph += item.toString();
			first = false;
		}
		context.write(key, new Text(outGraph));
	}
}
package stubs;

import java.io.*;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;

public class TopNListReducer extends Reducer<Text, Text, Text, Text> {
	
	private int N;
	private SortedMap<Double, String> topN = new TreeMap<Double, String>();
	
	@Before
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		this.N = conf.getInt("N", 10);
	}
	@Override
	public void reduce(Text key, Iterable<Text>values, Context context) 
			throws IOException , InterruptedException { //movieID,user	movies,correlation
		String group = key.toString();
		topN.clear();
		for (Text value : values) {
			String[] entries = value.toString().split(","); //movies correlation
			String movies = entries[0];
			double cor = Double.parseDouble(entries[1]); //0:movies 1:cor
			if (cor > 0 && cor <1.0000001) 
				topN.put(cor, group + "," + movies);//cor	movieID,user,movies
			if(topN.size() > N)
				topN.remove(topN.firstKey());
		}
		Text key1 = new Text(), value1 = new Text();
		for (double correlation : topN.keySet()) {
			String[] tokens = topN.get(correlation).split(","); //movieID,user,movies
			key1.set(tokens[0] + "," + tokens[1]);	
			value1.set(tokens[2] + "," + correlation);
			context.write(key1, value1);
		}
	}
}
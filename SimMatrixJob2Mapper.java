package stubs;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * Input Data made by RSJOb1 MapReduce
 * Receive:
 *		item_id1  item_id2  rating1  numRatings1  sumRatings1  rating2  numRatings2 sumRatings2
 * Emit:
 *		Key: item_id1  item_id2
 *		Value: rating1  rating2  numRatings1   sumRatings1  numRatings2  sumRatings2
 *
 */
public class SimMatrixJob2Mapper extends Mapper<LongWritable, Text, Text, Text>{
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] line = value.toString().split("\\s+");
		String itemID1 = line[0];
		String itemID2 = line[1];
		String others = line[2] + ' ' + line[5] + ' ' + line[3] + ' ' + line[4] + ' ' + line[6] + ' ' + line[7];
		context.write(new Text(itemID1 + ' ' + itemID2), new Text(others));
	}
	
}

package stubs;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * 
 * Input data made by dataPreProcessing.pig
 * Receive:
 * 		item_id  user_id  rating  numRatings  sumRatings
 * Emit:
 * 		Key: user_id		Value: item_id  rating  numRatings  sumRatings
 * 		
 */
public class RSJob1Mapper extends Mapper<LongWritable, Text, Text, Text>{
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] line = value.toString().split("\\s+");
		String userID = line[1];
		String others = line[0] + ' ' + line[2] + ' ' + line[3] + ' ' + line[4];
		context.write(new Text(userID), new Text(others));
	}
	
}

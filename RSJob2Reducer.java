package stubs;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * 
 * Receive:
 * 		Key: item_id1	item_id2		
 * 		Value: list of (rating1  rating2  numRatings1  sumRatings1 numRatings2 sumRatings2) pairs
 *		
 * Emit:
 *		Key: item_id1  item_id2		Value: Similarity
 *		 
 */	
public class RSJob2Reducer extends Reducer<Text, Text, Text, DoubleWritable> {
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {
		
		double NdotProd = 0;
		double Nrating1squaredSum = 0, Nrating2squaredSum = 0;
		double similarity = 0;
		int count = 0;
		
		for (Text value:values){
			String pair = value.toString();
			String[] pair1 = pair.split("\\s+");  //0:rating1 1:rating2 2:numRatings1 3:sumRatings1 4:numRatings2 5:sumRatings2
			double avrRating1 = Double.parseDouble(pair1[3]) / Double.parseDouble(pair1[2]);
			double avrRating2 = Double.parseDouble(pair1[5]) / Double.parseDouble(pair1[4]);
			NdotProd += (Double.parseDouble(pair1[0]) - avrRating1) * (Double.parseDouble(pair1[1]) - avrRating2);
			Nrating1squaredSum += Math.pow((Double.parseDouble(pair1[0]) - avrRating1), 2);
			Nrating2squaredSum += Math.pow((Double.parseDouble(pair1[1]) - avrRating2), 2);
			count++;
		}

		if (count == 1 || Nrating1squaredSum == 0 || Nrating2squaredSum == 0){
			context.write(key, new DoubleWritable(-0.5));
			return;
		}
		
		similarity = NdotProd/ (Math.sqrt(Nrating1squaredSum) * Math.sqrt(Nrating2squaredSum));
		context.write(key, new DoubleWritable(similarity));

	}

}

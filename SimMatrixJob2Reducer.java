package stubs;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;
/**
 * 
 * Receive:
 * 		Key: item_id1  item_id2
 *		Value: list of (rating1  rating2  numRatings1   sumRatings1  numRatings2  sumRatings2) pairs
 * Emit:
 *		Key: item_id1  item_id2
 *		Value: Similarity
 * 
 */
public class SimMatrixJob2Reducer extends Reducer<Text, Text, Text, DoubleArrayWritable> {
	private BufferedReader br;
	private Map<String, Integer> movieIndex = new HashMap<String, Integer>();
	private Map<String, DoubleWritable[]> Similarity = new HashMap<String, DoubleWritable[]>();
	Text key1 = new Text();

	@Before
	public void setup(Context context) throws IOException, InterruptedException {
		this.br = new BufferedReader(new FileReader("MovieIndex"));
		String line;
		int count = 0;
		while ((line = br.readLine()) != null) {
			movieIndex.put(line, count++);
		}
		br.close();
	}

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {

		double NdotProd = 0;
		double Nrating1squaredSum = 0, Nrating2squaredSum = 0;
		DoubleWritable similarity = new DoubleWritable();

		for (Text value:values){
			String pair = value.toString();
			String[] pair1 = pair.split("\\s+");  //0:rating1 1:rating2 2:numRatings1 3:sumRatings1 4:numRatings2 5:sumRatings2
			double avrRating1 = Double.parseDouble(pair1[3]) / Double.parseDouble(pair1[2]);
			double avrRating2 = Double.parseDouble(pair1[5]) / Double.parseDouble(pair1[4]);
			NdotProd += (Double.parseDouble(pair1[0]) - avrRating1) * (Double.parseDouble(pair1[1]) - avrRating2);
			Nrating1squaredSum += Math.pow((Double.parseDouble(pair1[0]) - avrRating1), 2);
			Nrating2squaredSum += Math.pow((Double.parseDouble(pair1[1]) - avrRating2), 2);
		}

		String[] keyArray = key.toString().split("\\s+"); //0:item_id1 1:item_id2
		DoubleWritable[] sim;
		if (Similarity.containsKey(keyArray[0]))
			sim = Similarity.get(keyArray[0]);
		else 
			sim = new DoubleWritable[movieIndex.size()];

		if (Nrating1squaredSum != 0 && Nrating2squaredSum != 0){
			similarity.set(NdotProd / (Math.sqrt(Nrating1squaredSum) * Math.sqrt(Nrating2squaredSum)));
			int index = movieIndex.get(keyArray[1]);
			sim[index] = similarity;
			Similarity.put(keyArray[0], sim);
		}

	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		for (String movie : Similarity.keySet()) {
			key1.set(movie);
			DoubleWritable[] temp = Similarity.get(movie);
			context.write(key1, new DoubleArrayWritable(DoubleWritable.class, temp));
		}
	}

}

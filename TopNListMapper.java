package stubs;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;

public class TopNListMapper extends Mapper<LongWritable, Text, Text, Text> {
	private BufferedReader br;
//	private SortedMap<Double, String> topN = new TreeMap<Double, String>();
	private Map<String, HashSet<String>> listB = new HashMap<String, HashSet<String>>();
	private Map<String, double[]> correlation = new HashMap<String, double[]>();
	private Map<String, Integer> movieIndex = new HashMap<String, Integer>();
	@Before
	public void setup(Context context) throws IOException , InterruptedException{
//		Configuration conf = context.getConfiguration();
		this.br = new BufferedReader(new FileReader("MovieIndex"));
		String line;
		int count = 0;
		while ((line = br.readLine()) != null) {
			movieIndex.put(line, count++);
		}
		this.br = new BufferedReader(new FileReader("ListB")); //ListB: key users, value the movie this user watched
		while ((line = br.readLine()) != null) {
			String[] words = line.split("\\W+");
			HashSet<String> movies = new HashSet<String>();
			for (int i = 1; i<words.length; i++) 
				movies.add(words[i]);
			listB.put(words[0], movies);
		}
		this.br = new BufferedReader(new FileReader("correlation"));
		while ((line = br.readLine()) != null) {
			String[] lines = line.split("\t");
			String[] words = lines[1].split(", ");
			double[] corr = new double[count];
			if (correlation.containsKey(lines[0])) 
				corr = correlation.get(lines[0]);
			String num;
			for (int i = 0; i<words.length; i++) {
				if ( i == 0) 
					num = words[i].replace("[", "");
				else if ( i == words.length-1)
					num = words[i].replace("]", "");
				else num = words[i];
				if (!num.equals("null")) 
					corr[i] = Double.parseDouble(num);
			}
			correlation.put(lines[0], corr);
		}
		br.close();
	}
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  // input is Validation Set or Test set, movieID, user, rating
	  // output movieID,user	movies,correlation
	  String[] words = value.toString().split("\t"); //movieID, user, rating
	  String movieID = words[0], userID = words[1];
	  Text key1 = new Text(), value1 = new Text();
	  key1.set(movieID + "," + userID);
	  double[] cormovie = correlation.get(movieID);
	  HashSet<String> moviesList = listB.get(userID); //user watched
	  double cor;
	  int index;
	  if (!moviesList.isEmpty() && !movieID.equals("11344")) {
		  for (String movies : moviesList) { //all movie user watched
			  index = movieIndex.get(movies); 
			  if (movieIndex.containsKey(movies)) {
				  if (cormovie[index] > 0) {
				  cor = cormovie[index]; //similarity
				  value1.set(movies + "," + cor);
				  context.write(key1, value1); //movieID,user	movies,correlation
				  }
			  }
		  }
	  }
	}
}

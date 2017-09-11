package stubs;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * 
 * Receive:
 * 		Key: user_id		Value: list of (item_id  rating  numRatings  sumRatings) pairs
 *		
 * Emit:
 *		Key: item_id1  item_id2		Value: rating1  numRatings1  sumRatings1  rating2  numRatings2 sumRatings2
 *		 
 */	
public class SimMatrixJob1Reducer extends 
Reducer<Text, Text, Text, Text> {

	//private Map<String, String> itemIDStats = new HashMap<String, String>();
    
	public static Comparator<String> ORDER = new Comparator<String>(){
		public int compare(String s1, String s2){
			return Integer.parseInt(s1.split("\\s+")[0]) - Integer.parseInt(s2.split("\\s+")[0]);
		}
	};
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {
		
//		Map<String, String> itemIDStats = new HashMap<String, String>();
		List<String> listPerUser = new ArrayList<String>();
		
		for (Text value: values){
			String pair = value.toString();
			listPerUser.add(pair);
			//String[] pair1 = pair.split("\\s+"); //0:itemID  1:rating  2:numRatings  3:sumRatings
			//String itemID = pair1[0];
			//String stats = pair1[1] + " " + pair1[2] + " " + pair1[3];
//			itemIDStats.put(itemID, stats);
		}
		
		Collections.sort(listPerUser, ORDER);
		
		for (int i = 0; i < listPerUser.size(); i++){
			for (int j = 0; j < listPerUser.size(); j++){
				String[] item1 = listPerUser.get(i).split("\\s+"); //0:itemID  1:rating  2:numRatings  3:sumRatings
				String[] item2 = listPerUser.get(j).split("\\s+"); 
				String itemID1 = item1[0];
				String itemID2 = item2[0];
				String stats1 = item1[1] + " " + item1[2] + " " + item1[3];
				String stats2 = item2[1] + " " + item2[2] + " " + item2[3];
				context.write(new Text(itemID1 + " " + itemID2), new Text(stats1 + " " + stats2));
			}
		}
		
//		Iterator<Entry<String, String>> iterator1 = itemIDStats.entrySet().iterator();
//		while (iterator1.hasNext()){
//			Map.Entry<String, String> entry1 = (Map.Entry<String, String>) iterator1.next();
//			Iterator<Entry<String, String>> iterator2 = itemIDStats.entrySet().iterator();
//			while (iterator2.hasNext()){
//				Map.Entry<String, String> entry2 = (Map.Entry<String, String>) iterator2.next();
//				context.write(new Text(entry1.getKey() + " " + entry2.getKey()), new Text(entry1.getValue() + " " + entry2.getValue()));
//			}
//		}

	}

//	@Override
//	public void cleanup(Context context) 
//			throws IOException, InterruptedException {
//
//		Iterator<Entry<String, String>> iterator1 = itemIDStats.entrySet().iterator();
//		while (iterator1.hasNext()){
//			Map.Entry<String, String> entry1 = (Map.Entry<String, String>) iterator1.next();
//			Iterator<Entry<String, String>> iterator2 = itemIDStats.entrySet().iterator();
//			while (iterator2.hasNext()){
//				Map.Entry<String, String> entry2 = (Map.Entry<String, String>) iterator2.next();
//				context.write(new Text(entry1.getKey() + " " + entry2.getKey()), new Text(entry1.getValue() + " " + entry2.getValue()));
//			}
//		}
//
//	}

}

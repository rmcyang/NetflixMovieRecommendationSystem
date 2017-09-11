package stubs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

public class TopNListDriver extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int exitCode = ToolRunner.run(conf, new TopNListDriver(), args); 
		System.exit(exitCode);
		}
    /*
     * Validate that two arguments were passed from the command line.
     */
	public int run(String[] args) throws Exception {
	    if (args.length != 2) {
	      System.out.printf("Usage: TopNListDriver %s [generic options] <input dir> <output dir>\n", getClass().getSimpleName());
	      return -1;
	    }


	Configuration conf = getConf();
	Path finalout=new Path(args[1]);
	
	Job job1 = new Job(conf);
	job1.setJobName("Job1 Aggregater");
	job1.setJarByClass(TopNListDriver.class);
	
	FileInputFormat.setInputPaths(job1, new Path(args[0]));
//    FileOutputFormat.setOutputPath(job1, new Path("temp1"));
	FileOutputFormat.setOutputPath(job1, finalout);
//	job1.setInputFormatClass(KeyValueTextInputFormat.class);
	job1.setMapperClass(TopNListMapper.class);
//    job1.setCombinerClass(TopNListReducer.class);
//    job1.setNumReduceTasks(2);
    job1.setReducerClass(TopNListReducer.class);
    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(Text.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);
    job1.waitForCompletion(true);
    
    boolean success = job1.waitForCompletion(true);
    if (success) {
//        finalout.getFileSystem(conf).delete(temp1, true);
//        finalout.getFileSystem(conf).delete(temp2, true);
    	return 0;
    }else return 1;
	}
}


package stubs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

public class ProjectDriver extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int exitCode = ToolRunner.run(conf, new ProjectDriver(), args); 
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
	
	Job job1 = new Job(conf);
	job1.setJobName("Job1 Aggregater");
	job1.setJarByClass(ProjectDriver.class);
	
	FileInputFormat.setInputPaths(job1, new Path(args[0]));
    FileOutputFormat.setOutputPath(job1, new Path("temp1"));
    
    job1.setMapperClass(RSJob1Mapper.class);
    job1.setReducerClass(RSJob2Reducer.class);
    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(Text.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);

    job1.waitForCompletion(true);
	
    Job job2 = new Job(conf);

    job2.setJobName("Job2 GetSimilarity");
    FileInputFormat.setInputPaths(job2, new Path("temp1"));
    FileOutputFormat.setOutputPath(job2, new Path("temp2"));
    
    
    job2.setMapperClass(RSJob2Mapper.class);
    job2.setReducerClass(RSJob2Reducer.class);
    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(Text.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(DoubleWritable.class);
    job2.waitForCompletion(true);
    
    Job job3 = new Job(conf);
    job3.setJobName("Job3 TopNListDriver");
    
    FileInputFormat.setInputPaths(job3, new Path("temp2"));
    FileOutputFormat.setOutputPath(job3, new Path(args[1]));
    
    job3.setMapperClass(RSJob3Mapper.class);
    job3.setCombinerClass(RSJob3Reducer.class);
    job3.setReducerClass(RSJob3Reducer.class);
    job3.setMapOutputKeyClass(IntWritable.class);
    job3.setMapOutputValueClass(Text.class);
    job3.setOutputKeyClass(IntWritable.class);
    job3.setOutputValueClass(Text.class);
//    job3.setOutputFormatClass(SequenceFileOutputFormat.class);
    /*
     * Start the MapReduce job and wait for it to finish.
     * If it finishes successfully, return 0. If not, return 1.
     */
    boolean success = job3.waitForCompletion(true);
    return success ? 0 : 1;
	}
}


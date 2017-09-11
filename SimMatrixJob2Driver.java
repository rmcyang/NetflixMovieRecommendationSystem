package stubs;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SimMatrixJob2Driver  extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		
		if (args.length != 2) {
			System.out.printf("usage SimMatrixJob2Driver <input> <output>");
			System.exit(1);
		}
		
		Job job = new Job(getConf());
		job.setJarByClass(SimMatrixJob2Driver.class);
		job.setJobName("SimMatrixJob2Driver");

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		//job.setOutputValueClass(DoubleWritable.class);
		job.setOutputValueClass(DoubleArrayWritable.class);

		job.setMapperClass(SimMatrixJob2Mapper.class);
		job.setReducerClass(SimMatrixJob2Reducer.class);
		//job.setNumReduceTasks(0);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean status = job.waitForCompletion(true);
		return status ? 0 : 1;
		
	}

	public static void main(String[] args) throws Exception {
		int returnStatus = ToolRunner.run(new SimMatrixJob2Driver(), args);
		System.exit(returnStatus);
	}
}


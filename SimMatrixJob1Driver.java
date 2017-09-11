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

public class SimMatrixJob1Driver  extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.printf("usage SimMatrixJob1 <input> <output>");
			System.exit(1);
		}
		
		Job job = new Job(getConf());
		job.setJarByClass(SimMatrixJob1Driver.class);
		job.setJobName("SimMatrixJob1Driver");

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(SimMatrixJob1Mapper.class);
		job.setReducerClass(SimMatrixJob1Reducer.class);
		//job.setNumReduceTasks(0);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean status = job.waitForCompletion(true);
		return status ? 0 : 1;
		
	}

	public static void main(String[] args) throws Exception {
		int returnStatus = ToolRunner.run(new SimMatrixJob1Driver(), args);
		System.exit(returnStatus);
	}
	
}


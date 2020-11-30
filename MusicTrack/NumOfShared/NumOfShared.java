import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class NumOfShared
{
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>
    {
        private Text TrackId = new Text();
		
        public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException
        {
            StringTokenizer tokens = new StringTokenizer(value.toString(), ",");

			// Bo qua token dau tien (UserID)
			tokens.nextToken();
			// Token thu 2 3 la TrackID va so luot share (shared)
            TrackId.set(tokens.nextToken().trim() + ",");
			int shared = Integer.parseInt(tokens.nextToken().trim());
			context.write(TrackId, new IntWritable(shared));
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>
    {	
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException
        {
			int count = 0;
		
            for(IntWritable value:values)
            {
                // Tinh tong so luot Share (shared) cua moi trackID
				count = count + value.get();
            }
			
			context.write(key, new IntWritable(count));
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = new Job(conf,"NumOfShared");
        job.setJarByClass(NumOfShared.class);
		
		job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
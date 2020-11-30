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
public class NumOfListenOnRadio
{
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>
    {
        private Text TrackId = new Text();
		
        public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException
        {
            StringTokenizer tokens = new StringTokenizer(value.toString(), ",");
			
			tokens.nextToken();
			TrackId.set(tokens.nextToken().trim() + ",");
			tokens.nextToken();
			int IsListenOnRadio = Integer.parseInt(tokens.nextToken().trim());
					
			context.write(TrackId, new IntWritable(IsListenOnRadio));
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>
    {	
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException
        {
			int count = 0;
		
            for(IntWritable value:values)
            {
				count = count + value.get();
            }
			
			context.write(key, new IntWritable(count));
        }
    }
    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = new Job(conf,"NumOfListenOnRadio");
        job.setJarByClass(NumOfListenOnRadio.class);
		
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
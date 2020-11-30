import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
public class MaxTemp
{
    public static class Map extends Mapper<LongWritable, Text, Text, FloatWritable>
    {
        private Text year = new Text();
		
        public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException
        {
            StringTokenizer tokens = new StringTokenizer(value.toString(), ",");
			
			year.set(tokens.nextToken().trim() + ",");
			float temp = Float.parseFloat(tokens.nextToken().trim());
				
			context.write(year, new FloatWritable(temp));
        }
    }

    public static class Reduce extends Reducer<Text, FloatWritable, Text, FloatWritable>
    {	
        public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException,InterruptedException
        {
			float MaxTemp = Float.MIN_VALUE;
		
            for(FloatWritable value:values)
            {
				float temp = value.get();
				
				if (temp > MaxTemp)
				{
					MaxTemp = temp;
				}
            }
			
			context.write(key, new FloatWritable(MaxTemp));
        }
    }
    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = new Job(conf,"MaxTemp");
        job.setJarByClass(MaxTemp.class);
		
		job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
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
            StringTokenizer tokens = new StringTokenizer(value.toString(), ","); // Chuyển value thành String, delimiter là ","
			
			year.set(tokens.nextToken().trim() + ","); // Tách token đầu để set giá trị Year
			float temp = Float.parseFloat(tokens.nextToken().trim()); // Chuyển nhiệt độ từ dạng String sang Float từ token tiếp theo
				
			context.write(year, new FloatWritable(temp)); // kết quả key là là year và value là Temp
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
				// Tìm ra giá trị Temp lớn nhất
				if (temp > MaxTemp)
				{
					MaxTemp = temp;
				}
            }
			
			context.write(key, new FloatWritable(MaxTemp)); // kết quả trả về là Key và giá trị nhiệt độ lớn nhất
        }
    }
    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = new Job(conf,"MaxTemp"); // Tạo job object
        job.setJarByClass(MaxTemp.class); // set jar file mà mỗi node sẽ tìm Mapper và Reducer class
		
		job.setInputFormatClass(TextInputFormat.class);  // set input format
        job.setOutputFormatClass(TextOutputFormat.class); // set output format
        FileInputFormat.addInputPath(job, new Path(args[0])); // thêm một đường dẫn vào danh sách input  cho MapReduce job
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // Đường dẫn đến thư mục chứa kết quả
		
		job.setMapperClass(Map.class); // set mapper class
		job.setCombinerClass(Reduce.class); // set combiner class
		job.setReducerClass(Reduce.class); // set reducer class 
		
        job.setOutputKeyClass(Text.class); // set key class cho output
        job.setOutputValueClass(FloatWritable.class); // set value class cho output
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1); //  giá trị 1 cho mỗi value
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString()); // tách các token từ value nhận được
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken()); // word được set giá trị là mỗi token 
        context.write(word, one); // Với mỗi token kết quả trả về là token đó và giá trị 1
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get(); // với input value nhận được ta tính tổng các value
      }
      result.set(sum);
      context.write(key, result); // kết quả trả về là key ban đầu và tổng các values
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count"); // Tạo job object
    job.setJarByClass(WordCount.class);  // set jar file mà mỗi node sẽ tìm Mapper và Reducer class
    job.setMapperClass(TokenizerMapper.class); // set mapper class
    job.setCombinerClass(IntSumReducer.class); // set combiner class
    job.setReducerClass(IntSumReducer.class); // set reducer class
    job.setOutputKeyClass(Text.class);   // set key class cho output
    job.setOutputValueClass(IntWritable.class);  // set value class cho output
    FileInputFormat.addInputPath(job, new Path(args[0])); // thêm một đường dẫn vào danh sách input  cho MapReduce job
    FileOutputFormat.setOutputPath(job, new Path(args[1]));// Đường dẫn đến thư mục chứa kết quả
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
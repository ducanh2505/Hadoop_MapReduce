import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashSet;
import java.util.Set;
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
import java.text.SimpleDateFormat;
import java.util.Date;
import java.text.ParseException;

public class STDSubscribers {
    public static int fromPhoneNumber = 0;
    public static int toPhoneNumber = 1;
    public static int callStartTime = 2;
    public static int callEndTime = 3;
    public static int STDFlag = 4;

    public static class TokenizerMapper extends Mapper < Object, Text, Text, LongWritable > {
        Text phoneNumber = new Text();
        LongWritable durationInMinutes = new LongWritable();
        public void map(Object key, Text value, Mapper < Object, Text, Text, LongWritable > .Context context)
        throws IOException,
        InterruptedException {
            // tach token theo ki tu |
            String[] parts = value.toString().split("[|]");

            // Xet nhung mau du lieu co STDFlag = 1
            if (parts[STDSubscribers.STDFlag].equalsIgnoreCase("1")) {
                // gan cac gia tri cho cac bien phoneNumber, callStartTime, callEndTime
                phoneNumber.set(parts[STDSubscribers.fromPhoneNumber]);
                String callEndTime = parts[STDSubscribers.callEndTime];
                String callStartTime = parts[STDSubscribers.callStartTime];
                // Tinh duration bang ham toMillis
                long duration = toMillis(callEndTime) - toMillis(callStartTime);
                // Chuyen ms thanh minutes
                durationInMinutes.set(duration / (1000 * 60));
                context.write(phoneNumber, durationInMinutes);
            }
        }
        // ham toMillis chuyen date thanh ms
        private long toMillis(String date) {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date dateFrm = null;
            try {
                dateFrm = format.parse(date);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            return dateFrm.getTime();
        }
    }

    public static class SumReducer extends Reducer < Text, LongWritable, Text, LongWritable > {
        private LongWritable result = new LongWritable();
        public void reduce(Text key, Iterable < LongWritable > values, Reducer < Text, LongWritable, Text, LongWritable > .Context context)
        throws IOException,
        InterruptedException {
            long sum = 0;
            for (LongWritable val: values) {
                // tinh tong so phut goi di (durationInMinutes) cua moi phoneNumber
                sum += val.get();
            }
            this.result.set(sum);
            if (sum >= 60) {
                // Ghi cac phoneNumber co tong so phut (durationInMinutes) >= 60
                context.write(key, this.result);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length != 2) {
            System.err.println("Usage: stdsubscriber < in>< out>");
            System.exit(2);
        }
        Job job = new Job(conf, "STD Subscribers");
        job.setJarByClass(STDSubscribers.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(SumReducer.class);
        job.setReducerClass(SumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
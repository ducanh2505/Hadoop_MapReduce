import java.util.Iterator;
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


public class WeatherData {
    public static class MaxTemperatureMapper extends  Mapper < Object, Text, Text, Text > {
        @Override
        public void map(Object arg0, Text Value, Context context) throws IOException, InterruptedException {
            
            String line = Value.toString();
            // Example of Input
            // Date Max Min
            // 25380 20130101 2.514 -135.69 58.43 8.3 1.1 4.7 4.9 5.6 0.01 C 1.0 -0.1 0.4 97.3 36.0 69.4
            //-99.000 -99.000 -99.000 -99.000 -99.000 -9999.0 -9999.0 -9999.0 -9999.0 -9999.0
            String date = line.substring(6, 14);
            float temp_Max = Float.parseFloat(line.substring(39, 45).trim());
            float temp_Min = Float.parseFloat(line.substring(47, 53).trim());

            if (temp_Max > 40.0) {
                // Hot day
                context.write(new Text("Hot Day " + date), new Text(String.valueOf(temp_Max)));
            }
            
            if (temp_Min < 10) {
                // Cold day
                context.write(new Text("Cold Day " + date), new Text(String.valueOf(temp_Min)));
            }
        }
    }
    public static class MaxTemperatureReducer extends Reducer < Text, Text, Text, Text > {
        
        @Override
        public void reduce(Text Key, Iterable < Text > values, Context context) throws IOException, InterruptedException {
            
            // defining a local variable min value of int
            double maxValue = Float.MIN_VALUE;


            //iterating through all the temperature and forming the key value pair
            for (Text value : values) {
                // convert temperatures from Text to String
                String string_value = value.toString();

                // convert temperatures from String to Float
                double float_value = Float.parseFloat(string_value);

                // Dumping the output
                maxValue = Math.max(maxValue, float_value);
            }
            //Sending to output collector which in turn passes the same to reducer
            //So in this case the output from mapper will be the length of a word and that word
            context.write(Key, new Text(String.valueOf(maxValue)));
        }
    }
    /*
     * @method main
     * <p>This method is used for setting all the configuration properties.
     * It acts as a driver for map reduce code.
     * @return void
     * @method_arguments args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        //reads the default configuration of cluster from the configuration xml files
        Configuration conf = new Configuration();

        //Initializing the job with the default configuration of the cluster
        Job job = new Job(conf, "temp");

        //Assigning the driver class name
        job.setJarByClass(WeatherData.class);

        //Defining the output key class for the mapper
        job.setMapOutputKeyClass(Text.class);

        //Defining the output value class for the mapper
        job.setMapOutputValueClass(Text.class);

        //Defining the output key class for the final output i.e. from reducer
        job.setOutputKeyClass(Text.class);

        //Defining the output value class for the final output i.e. from reduce
        job.setOutputValueClass(Text.class);

        //Defining the mapper class name
        job.setMapperClass(MaxTemperatureMapper.class);
        // job.setCombinerClass(Reduce.class);

        //Defining the reducer class name
        job.setReducerClass(MaxTemperatureReducer.class);
        
        //Defining input Format class which is responsible to parse the dataset into a key value pair
        job.setInputFormatClass(TextInputFormat.class);

        /*
         * Defining output Format class which is responsible to parse the final key-value output 
         * from MR framework to a text file into the hard disk
         */
        job.setOutputFormatClass(TextOutputFormat.class);

        //Configuring the input/output path from the filesystem into the job
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //deleting the output path automatically from hdfs so that we don't have delete it explicitly
        outputPath.getFileSystem(conf).delete(outputPath);

        //exiting the job only if the flag value becomes false
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
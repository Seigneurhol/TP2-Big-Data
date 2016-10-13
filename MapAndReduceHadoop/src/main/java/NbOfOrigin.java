/**
 * Created by Seigneurhol on 12/10/2016.
 */
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class NbOfOrigin {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        //map and reduce function are called for each line
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = ""; //Initialize the line String
            line = value.toString(); //Store the line from the file in a String variable
            String[] parts = line.split(";"); //Split to get the full origin between ";"
            String[] parts2 = parts[2].split(", ?"); //Split with a regex to filter ", " and "," to get each different origin
            int nbOfOrigin_int = parts2.length; //Get the size of nb of origin of a first name
            String nbOfOrigin = Integer.toString(nbOfOrigin_int); //Convert it to a String to send it to word
            word.set(nbOfOrigin); //Get the origin part of the line
            context.write(word,one); //Write the result (number of origin per first name) into a Context variable
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0; //Initialize a sum variable
            //For the same key count the number of value
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum); //Set the final result : the number of occurance of a key
            context.write(key, result); //Write the result (number of first name by number of origin) into a Context variable
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(OriginCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Date;

public class WordCount {
    public static class String2CountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        IntWritable one = new IntWritable(1);

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            // Running this on wikipedia archives, removing these fields makes a cleaner output
            line = line.replaceAll("</anchor>", "");
            line = line.replaceAll("<link>", "");
            line = line.replaceAll("</sublink>", "");
            line = line.replaceAll("</link>", "");
            line = line.replaceAll("<sublink linktype=\"nav\">", "");
            line = line.replaceAll("<anchor>", "");

            String[] words = line.split(" ");
            for (String word : words) {
                // Emit a separate dummy counter for each word
                context.write(new Text(word.toLowerCase().replaceAll("[,.!?\\-]", "")), one);
            }
        }
    }

    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable val : values) {
                // Sum the number of occurrences of each word
                count += val.get();
            }

            context.write(key, new IntWritable(count));
        }
    }

    public static void main(String args[]) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Word_Count");
        job.setJarByClass(WordCount.class);

        job.setMapperClass(String2CountMapper.class);
        job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Input should be text files stored in the "input" directory on the hdfs
        FileInputFormat.setInputPaths(job, new Path("input"));
        FileOutputFormat.setOutputPath(job, new Path("output/" + new Date().getTime()));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Grep {
    public static class File2OccurrenceMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Pattern pattern;

        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            String regex = conf.get("regex.pattern");
            pattern = Pattern.compile(regex);
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException  {
            String line = value.toString();
            Matcher matcher = pattern.matcher(line);
            String filename = ((FileSplit) context.getInputSplit()).getPath().toString();

            while (matcher.find()) {
                // Emits each filename -> "<line number>:<matching string>
                context.write(new Text(filename), new Text(key.toString() + ":" + line));
            }
        }
    }

    public static class File2OccurrencesReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException  {
            List returnList = new ArrayList();
            value.forEach(returnList::add);

            context.write(key, new Text(returnList.toString()));
        }
    }

    public static void main(String args[]) throws Exception {
        Configuration conf = new Configuration();
        conf.set("regex.pattern", args[0]);
        Job job = Job.getInstance(conf, "Grep");
        job.setJarByClass(Grep.class);

        job.setMapperClass(File2OccurrenceMapper.class);
        job.setCombinerClass(File2OccurrencesReducer.class);
        job.setReducerClass(File2OccurrencesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("input"));
        FileOutputFormat.setOutputPath(job, new Path("output/grep/" + new Date().getTime()));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
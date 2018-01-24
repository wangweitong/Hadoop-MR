package MR;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import outputformat.ReOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class MRWordCount {
    public static class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text text = new Text();
        private IntWritable intWritable=new IntWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] strs = value.toString().split(" ");
            for (String s : strs) {
                text.set(s);
                context.write(text,intWritable);
            }

        }
    }

    public static class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable intWritable = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable t : values) {
                sum=sum+1;
            }
            intWritable.set(sum);
            context.write(key, intWritable);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Job job = Job.getInstance();
        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job, new Path("E:\\in\\MRWordCount"));//设置输入的数据的路径
        job.setOutputFormatClass(ReOutputFormat.class);

        Path path = new Path("E:\\out\\MRWordCount");//设置输出数据的路径
        FileSystem fs = FileSystem.get(new URI("file:///E://out//MRWordCount"), new Configuration());
        if (fs.exists(path)) {//如果存在
            fs.delete(path, true);
            FileOutputFormat.setOutputPath(job, path);
            job.waitForCompletion(true);
        }

    }
}

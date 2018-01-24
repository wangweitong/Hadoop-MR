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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class PhoneCount {
    public static class PhoneMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text text = new Text();
        private IntWritable intWritable = new IntWritable();
        int a = 0;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strs = value.toString().split("\t");
            a = Integer.parseInt(strs[8]) + Integer.parseInt(strs[9]);
            text.set(strs[1]);
            intWritable.set(a);
            context.write(text, intWritable);
        }
    }

    public static class PhoneReducer extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        private Text text=new Text();
        private IntWritable intWritable=new IntWritable();
        int sum=0;
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable i:values)
            {
                sum=sum+i.get();
            }
            text.set(key);
            intWritable.set(sum);
            context.write(text,intWritable);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Job job=Job.getInstance();
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapperClass(PhoneMapper.class);
        job.setReducerClass(PhoneReducer.class);
        FileInputFormat.setInputPaths(job,new Path("E:\\phoneflow\\phone1.dat"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\phoneout"));
        FileSystem fs=FileSystem.get(new URI("file://E://phoneout"),new Configuration());
        if(fs.exists(new Path("E:\\phoneout")))
        {
            fs.delete(new Path("E:\\phoneout"),true);
        }
        job.waitForCompletion(true);
    }
}

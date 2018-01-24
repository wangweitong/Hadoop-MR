package MR;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

public class FindAttentionMR {
    public static class FAMapper extends Mapper<LongWritable,Text,Text,Text>
    {
        private Text text=new Text();
        private Text text2=new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strs = value.toString().split(":");
            text2.set(strs[0]);
            String[] strs2 = strs[1].split(",");
            for (String s : strs2)
            {
                text.set(s);
                context.write(text,text2);
            }

        }
    }
    public static class FAReducer extends Reducer<Text,Text,Text,Text>
    {
        private Text text=new Text();
        private Text text2=new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            text.set(key.toString()+":");
            StringBuilder sb=new StringBuilder();
            for (Text t:values)
            {
                sb.append(t+"\t");
            }
            text2.set(sb.toString());
            context.write(text,text2);
        }
    }
    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        Job job= Job.getInstance();
        job.setMapperClass(FAMapper.class);
        job.setReducerClass(FAReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job,new Path("E:\\in\\FindAttention"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\out\\FindAttention"));
        FileSystem fs=FileSystem.get(new URI("file:///E://out//FindAttention"),new Configuration());
        Path path=new Path("E://out//FindAttention");
        if (fs.exists(path))
        {
            fs.delete(path,true);
        }
        job.waitForCompletion(true);
    }
}

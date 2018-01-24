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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class ReverseIndex
{
    public static class RIMapper extends Mapper<LongWritable,Text,Text,Text>
    {
        private Text text=new Text();
        private Text text2=new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String [] strs=value.toString().split(" ");
            FileSplit fileSplit=(FileSplit)context.getInputSplit();
            String path=fileSplit.getPath().getName().toString();
            for (String s:strs)
            {
                text.set(s+"\t"+path);
                text2.set("1");
                context.write(text,text2);
            }

        }
    }
    public static class RICombiner extends Reducer<Text,Text,Text,Text>
    {
        private Text text=new Text();
        private Text text2=new Text();
        private int count=0;
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text t:values)
            {
                count=count+1;
            }
            String [] strs=key.toString().split("\t");
            text.set(strs[0]);
            text2.set(strs[1]+"-->"+count);
            context.write(text,text2);
        }
    }
    public static class RIReducer extends Reducer<Text,Text,Text,Text>
    {
        private Text text=new Text();
        private Text text2=new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb=new StringBuilder();
            for (Text t:values)
            {
                sb.append("\t"+t.toString());
            }
            text.set(key);
            text2.set(sb.toString());
            context.write(text,text2);
        }
    }

    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        Job job= Job.getInstance();
        job.setMapperClass(RIMapper.class);
        job.setCombinerClass(RICombiner.class);
        job.setReducerClass(RIReducer.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        FileInputFormat.setInputPaths(job,new Path("E:\\in\\InverseIndex"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\out\\InverseIndex"));
        FileSystem fs=FileSystem.get(new URI("file://E://out//InverseIndex"),new Configuration());
        Path path=new Path("E://out//InverseIndex");
        if(fs.exists(path))
        {
            fs.delete(path,true);
        }
        job.waitForCompletion(true);
    }
}

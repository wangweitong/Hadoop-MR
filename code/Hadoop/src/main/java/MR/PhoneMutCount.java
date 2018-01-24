package MR;

import entity.WordTimes;
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
import partitioner.PhonePartition;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class PhoneMutCount {
    public static class PhoneMapper extends Mapper<LongWritable,Text,Text, WordTimes>
    {
        private Text text=new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String [] strs=value.toString().split("\t");
            text.set(strs[1]);
            int upflow=Integer.parseInt(strs[8]);
            System.out.println(upflow);
            int downflow=Integer.parseInt(strs[9]);
            int allflow=upflow+downflow;

            WordTimes wordTimes=new WordTimes(upflow,downflow,allflow,strs[1]);
            context.write(text,wordTimes);
        }
    }
    public static class PhoneReducer extends Reducer<Text,WordTimes,Text,WordTimes>
    {
        int upflow=0;
        int downflow=0;

        @Override
        protected void reduce(Text key, Iterable<WordTimes> values, Context context) throws IOException, InterruptedException {
            for (WordTimes wt:values)
            {
                upflow=upflow+wt.getUpflow();
                downflow=downflow+wt.getDownflow();
            }
            int allflow=upflow+downflow;
            WordTimes wordTimes=new WordTimes(upflow,downflow,upflow+downflow,key.toString());
            context.write(key,wordTimes);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Job job= Job.getInstance();
        job.setMapperClass(PhoneMapper.class);
        job.setReducerClass(PhoneReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(WordTimes.class);
        job.setPartitionerClass(PhonePartition.class);
        job.setSortComparatorClass(TopN.TopNOrder.class);
        job.setNumReduceTasks(5);
        FileOutputFormat.setOutputPath(job,new Path("E:\\phoneout2"));
        FileInputFormat.setInputPaths(job,new Path("E:\\phoneflow\\phonein2"));
        FileSystem fs=FileSystem.get(new URI("file://E://phoneout2"),new Configuration());
        Path path=new Path("E:\\phoneout2");

        if (fs.exists(path))
        {
            fs.delete(path,true);
        }
        job.waitForCompletion(true);
    }
}

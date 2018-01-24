package MR;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TopN {
    public static class TopNMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Counter counter=context.getCounter("BigGroup","TestCounter");
            Counter counter = context.getCounter(Counter2.num);
            String line = value.toString();
            IntWritable intWritable = new IntWritable();
            counter.increment(100);
            Text text = new Text();
            Pattern pattern = Pattern.compile("\\w+");
            Matcher matcher = pattern.matcher(line);
            if (matcher.find()) {
                String word = matcher.group();
                text.set(word);
                intWritable.set(1);
                context.write(text, intWritable);

            }

        }
    }

    public static enum Counter2 {num}

    public static class TopNCombiner extends Reducer<Text, IntWritable, Text, IntWritable>
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
            intWritable.set(sum);
            context.write(key,intWritable);
        }
    }
    public static class TopNReduce extends Reducer<Text,IntWritable,Text,IntWritable>
    {
        private Text text=new Text();
        private  IntWritable intWritable=new IntWritable();
        int sum=0;
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable i:values)
            {
                sum=sum+i.get();
            }
            intWritable.set(sum);
            context.write(key,intWritable);
        }
    }
    public  static  class  TopNOrder extends Text.Comparator
    {

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            String str1=new String (b1,s1+1,l1-1);
            String str2=new String(b2,s2+1,l2-1);

            if(str1.length()==str2.length()){
                char [] a=str1.toCharArray();
                char [] b=str1.toCharArray();
                for (int i=0;i<a.length;i++)
                {
                    if (Character.compare(a[i],b[i])==0)
                    {
                        continue;
                    }
                    else return Character.compare(a[i],b[i]);

                }

            }
            return str1.length()-str2.length();
        }

    }

    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        Job job= Job.getInstance();
        job.setMapperClass(TopNMapper.class);
        job.setReducerClass(TopNReduce.class);
        job.setCombinerClass(TopNCombiner.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
       //job.setSortComparatorClass(TopNOrder.class);

        FileInputFormat.setInputPaths(job,new Path("E:\\in\\TopN1"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\out\\TopN1"));
        FileSystem fs=FileSystem.get(new URI("file://E://out//TopN1"),new Configuration());
        Path path=new Path("E:\\out\\TopN1");
        if(fs.exists(path))
        {
            fs.delete(path,true);
        }
        job.waitForCompletion(true);
    }
}

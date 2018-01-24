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

public class CSTest {
    public static class TM extends Mapper<LongWritable,Text,Text,IntWritable> {
        private Text text = new Text();
        private IntWritable intWritable = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line =value.toString();

            String str [] =line.split("\t");

            if(str.length==3){//过滤掉不满足条件的记录条
                //  context.write(new Text(str[0]),new IntWritable(Integer.parseInt(str[2])));
                text.set(str[0]);
                intWritable.set(Integer.parseInt(str[2]));
                context.write(text,intWritable);
            }
        }
    }
    public static class TR extends Reducer<Text,IntWritable,Text,IntWritable>
    {
        private Text text=new Text();
        private IntWritable intWritable=new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum=0;
            for(IntWritable i:values){
                sum+=i.get();
            }
            //结果应该为 班级编号,总分、
            String result="班级编号："+key.toString()+", 总分"+sum;
            text.set(result);
            intWritable.set(sum);
            context.write(text,intWritable);
        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Job job = Job.getInstance();
        job.setMapperClass(TM.class);
        job.setReducerClass(TR.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
//        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
//        FileOutputFormat.setCompressOutput(job,true);
        FileInputFormat.setInputPaths(job, new Path("E:\\1b.txt"));//设置输入的数据的路径
        Path path = new Path("E:\\1testWC");//设置输出数据的路径
        FileSystem fs = FileSystem.get(new URI("file:///E://1testCS"), new Configuration());
        if (fs.exists(path)) {//如果存在
            fs.delete(path, true);
            FileOutputFormat.setOutputPath(job, path);
            job.waitForCompletion(true);
        }

    }
}

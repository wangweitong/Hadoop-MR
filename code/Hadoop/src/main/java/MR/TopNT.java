package MR;

import entity.WT;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TopNT {
    public static class TopNTMapper extends Mapper<LongWritable,Text,WT,NullWritable> {
        private List<WT> list=new ArrayList<WT>(4);
        {
            for(int i=0;i<15;i++){
                list.add(new WT());
            }
        }
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Text text = new Text();


            String[] strs = value.toString().split("\t");
            WT wt = new WT(strs[0], Integer.parseInt(strs[1]));
            System.out.println(strs[0]+strs[1]);
            text.set(strs[0]);
            list.add(wt);
            Collections.sort(list);
            list.remove(15);

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (int i = 0; i < 14; i++) {
                context.write(list.get(i),NullWritable.get());
            }

        }
    }

    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        Job job= Job.getInstance();
        job.setMapperClass(TopNTMapper.class);

        job.setMapOutputKeyClass(WT.class);
        job.setMapOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
//        FileSystem fs=FileSystem.get(new URI(args[]),new Configuration());
//        Path path=new Path("E:\\out\\TopN");
//        if(fs.exists(path))
//        {
//            fs.delete(path,true);
//        }
        job.waitForCompletion(true);
    }

}

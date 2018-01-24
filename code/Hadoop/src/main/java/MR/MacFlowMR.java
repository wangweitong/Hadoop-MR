package MR;

import entity.MacFlow;
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
import java.util.HashMap;
import java.util.Map;

public class MacFlowMR {
    public static class MacFlowMapper extends Mapper<LongWritable,Text,Text, MacFlow>
    {
        private Text text=new Text();
        private MacFlow macFlow=new MacFlow();
        int a=0;
        int b=0;
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strs = value.toString().split("\t");
            int a = Integer.parseInt(strs[8]);
            int b = Integer.parseInt(strs[9]);
            text.set(strs[1]);
            macFlow.setFlow(a + b);
            macFlow.setMac(strs[2]);
            context.write(text, macFlow);
        }
    }
    public static class MacFlowReduce extends Reducer<Text,MacFlow,Text,Text>
    {
        @Override
        protected void reduce(Text key, Iterable<MacFlow> values, Context context) throws IOException, InterruptedException {
            Map<String,Integer> map=new HashMap<>();
            for (MacFlow mf:values)
            {
                String mac=mf.getMac();
                int flow=mf.flow;
                if(map.containsKey(mac) )
                {
                    map.put(mac,map.get(mac)+flow);
                }
                else map.put(mac,flow);
            }
            StringBuffer sb=new StringBuffer();
            for(Map.Entry entry:map.entrySet())
            {
                sb.append(entry.getKey().toString()+"\t"+entry.getValue().toString()+"\t");
            }
            Text text=new Text(sb.toString());
            context.write(key,text);
        }
    }

    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
       Job job=Job.getInstance();
       job.setMapperClass(MacFlowMapper.class);
       job.setReducerClass(MacFlowReduce.class);
       job.setMapOutputKeyClass(Text.class);
       job.setMapOutputValueClass(MacFlow.class);
//       job.setGroupingComparatorClass(TComparetor.class);
        FileInputFormat.setInputPaths(job,new Path("E:\\in\\MacFlow\\phone1.dat"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\out\\MacFlow"));
        Path path=new Path("E:\\out\\MacFlow");
        FileSystem fs=FileSystem.get(new URI("file://E://out//MacFlow"),new Configuration());
        if(fs.exists(path))
        {
            fs.delete(path,true);
        }
        job.waitForCompletion(true);
    }
}

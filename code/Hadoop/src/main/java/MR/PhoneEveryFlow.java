package MR;


import entity.Flow;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import outputformat.MyDBOutput;
import util.JobUtil;

import java.io.IOException;

/**
 * Created by 代俊朴 on 2018/1/17.
 */
public class PhoneEveryFlow {
    public static class ForMapper extends Mapper<LongWritable,Text,Text,Flow>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line=value.toString();
            String str  []  =line.split("\t");
            int upFlow=Integer.parseInt(str[8]);
            int downFlow=Integer.parseInt(str[9]);
            Flow flow=new Flow(str[1],upFlow,downFlow,upFlow+downFlow);
            context.write(new Text(str[1]),flow);
        }
    }
    public static class ForReducer extends Reducer<Text,Flow,Flow,NullWritable>{
        private Flow flow=new Flow();
        @Override
        protected void reduce(Text key, Iterable<Flow> values, Context context) throws IOException, InterruptedException {
            int sumUP=0;
            int sumDow=0;
            for(Flow f:values){
                sumUP+=f.getUpFlow();
                sumDow+=f.getDownFlow();
            }
            int sumAl=sumUP+sumDow;
            flow.setAllFlow(sumAl);
            flow.setDownFlow(sumDow);
            flow.setUpFlow(sumUP);
            flow.setPhoneNumber(key.toString());
            context.write(flow,NullWritable.get());
        }
    }
    public static void main(String[] args) {
        JobUtil.commitJob(PhoneEveryFlow.class,
                "E:\\in\\MacFlow\\phone1.dat",
                "",
                new MyDBOutput()
        );
    }
}


package partitioner;

import entity.WordTimes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PhonePartition extends Partitioner<Text, WordTimes> {

    @Override
    public int getPartition(Text text, WordTimes wordTimes, int i) {
        String s=text.toString().substring(0,3);
        if(s.equals("137"))
        {
            return 0;
        }
        else if (s.equals("135"))
        {
            return 1;
        }
        else if (s.equals("136"))
        {
            return 2;
        }
        else if(s.equals("138"))
        {
            return 3;
        }
        else return 4;
    }
}

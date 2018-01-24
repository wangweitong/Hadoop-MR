package comparetor;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TComparetor extends WritableComparator{
    public TComparetor() {
        super(Text.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return a.toString().substring(0,2).compareTo(b.toString().substring(0,2));
    }
}

package tc;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

public class TriangleReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(final Text key, final Iterable<Text> values, final Context context)
            throws IOException, InterruptedException {

        HashSet<String> EdgeSet = new HashSet<String>();
        for(Text t :values) {
            if (EdgeSet.contains(t.toString())){
                context.getCounter(RSJoinTriangleCount.COUNTER.TriangleCount).increment(1);
            }
            else{
                EdgeSet.add(t.toString());
            }
        }

    }

}

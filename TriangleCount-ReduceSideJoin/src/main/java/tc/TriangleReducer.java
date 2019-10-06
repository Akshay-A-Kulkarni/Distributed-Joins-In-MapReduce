package tc;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class TriangleReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(final Text key, final Iterable<Text> values, final Context context)
            throws IOException, InterruptedException {

        ArrayList<String> listT = new ArrayList<String>();
        ArrayList<String> listO = new ArrayList<String>();

        HashMap<String,Integer> Count = new HashMap<>();

        for (final Text val : values) {
            if (val.charAt(0) == 'B') {
                listT.add((val.toString().substring(1)));
            }
            else if (val.charAt(0) == 'A'){
                listO.add((val.toString().substring(1)));

            }
        }

        if (listO.size() != 0) {
            context.getCounter(RSJoinTriangleCount.COUNTER.TriangleCount).increment(listT.size());
        }

    //        HashSet<String> EdgeSet = new HashSet<String>();
    //        for(Text t :values) {
    //            if (EdgeSet.contains(t.toString())){
    //                context.getCounter(RSJoinTriangleCount.COUNTER.TriangleCount).increment(1);
    //            }
    //            else{
    //                EdgeSet.add(t.toString());
    //            }
    //        }

    }

}

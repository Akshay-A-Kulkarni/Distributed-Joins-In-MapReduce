package tc;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;


public class TriangleMapper extends Mapper<Object, Text, Text, Text> {
    private static final Text I = new Text("I");
    private static final Text O = new Text("O");

    private final Text E1 = new Text();
    private final Text E2 = new Text();
    private int MAX;

    private HashMap<String, ArrayList<String>> cachedEdges = new HashMap<String, ArrayList<String>>();

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        // Get the type of join from our configuration
        MAX = Integer.parseInt(context.getConfiguration().get("max.filter"));
        if (MAX == -1) {
            MAX = 11316811;
        }

        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles != null && cacheFiles.length > 0) {
            try
            {
                FileSystem fs = FileSystem.get(context.getConfiguration());
                Path path = new Path(cacheFiles[0].toString());
                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));

                String in;

                while ((in = reader.readLine()) != null){
                    final String[] line = in.toString().split(",");
                    if (Integer.parseInt(line[0]) <= MAX & Integer.parseInt(line[1]) <= MAX) {
                        if (cachedEdges.get(line[0]) == null) { //gets the value for an id)
                            cachedEdges.put(line[0], new ArrayList<String>()); //no ArrayList assigned, create new ArrayList
                            cachedEdges.get(line[0]).add(line[1]); //adds value to list.
                        }
                        else{
                            cachedEdges.get(line[0]).add(line[1]); //adds value to list.
                        }

                    }
                }
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }

    @Override
    public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
        // each map call gets one line i.e. one edge to process.
        // splitting the incoming edge represented by a line into nodes
        final String[] edge = value.toString().split(",");
        // Filter to disregard nodes above the max filter val
        if (Integer.parseInt(edge[0]) <= MAX & Integer.parseInt(edge[1]) <= MAX) {
            if (cachedEdges.containsKey(edge[1])) {
                for (String val : cachedEdges.get(edge[1])) {
                    if (cachedEdges.containsKey(val)) {
                        if (cachedEdges.get(val).contains(edge[0])) {
                            context.getCounter(RepJoinTriangleCount.COUNTER.TriangleCount).increment(1);
                        }
                    }
                }
            }
        }
    }
}


package tc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class TriangleCount extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(TriangleCount.class);
    private static final Text incoming = new Text("In");
    private static final Text outgoing = new Text("Out");
    private static int MAX = 99;

    public static class FollowerMapper extends Mapper<Object, Text, Text, Text> {
        private final Text F1 = new Text();
        private final Text F2 = new Text();

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            // each map call gets one line i.e. one edge to process.
            // splitting the incoming edge represented by a line into nodes
            final String[] edge = value.toString().split(",");
            if (Integer.parseInt(edge[0]) <= MAX & Integer.parseInt(edge[0]) <= MAX) {
                F1.set(edge[0]);
                F2.set(edge[1]);
                context.write(F1, incoming);
                context.write(F2, outgoing);
            }
        }
    }


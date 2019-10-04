package tc;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class TriangleCount extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(TriangleCount.class);

    @Override
    public int run(final String[] args) throws Exception {
        JobControl jobControl = new JobControl("jobChain");
        final Configuration conf1 = getConf();
        String[] otherArgs = new GenericOptionsParser(conf1, args)
                .getRemainingArgs();
        String MaxFilter = otherArgs[2];
        final Job job1 = Job.getInstance(conf1, "2-path Count");
        job1.setJarByClass(TriangleCount.class);
        final Configuration jobConf = job1.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", ",");
//		 Delete output directory, only to ease local development; will not work on AWS. ===========
        final FileSystem fileSystem = FileSystem.get(conf1);
        if (fileSystem.exists(new Path(args[1]))) {
            fileSystem.delete(new Path(args[1]), true);
        }
//		 ================
        job1.getConfiguration().set("max.filter", MaxFilter);
        job1.setMapperClass(TwoPathMapper.class);
        job1.setReducerClass(TwoPathReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]+"/temp"));

        ControlledJob controlledJob1 = new ControlledJob(conf1);
        controlledJob1.setJob(job1);


        // Adding Job to JobControl
        jobControl.addJob(controlledJob1);
        Configuration conf2 = getConf();

        Job job2 = Job.getInstance(conf2);
        job2.setJarByClass(TriangleCount.class);
        job2.setJobName("Triangle Count");
        job2.getConfiguration().set("max.filter", MaxFilter);

        FileInputFormat.setInputPaths(job2, new Path(args[1] + "/temp"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/final"));

        job2.setMapperClass(TwoPathMapper.class);
        job2.setReducerClass(TwoPathReducer2.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        // Setting Control job 2
        ControlledJob controlledJob2 = new ControlledJob(conf2);
        controlledJob2.setJob(job2);

        // make job2 dependent on job1
        controlledJob2.addDependingJob(controlledJob1);
        // add the job to the job control
        jobControl.addJob(controlledJob2);
        Thread jobControlThread = new Thread(jobControl);
        jobControlThread.start();

        while (!jobControl.allFinished()) {
            System.out.println("Jobs in waiting state: " + jobControl.getWaitingJobList().size());
            System.out.println("Jobs in ready state: " + jobControl.getReadyJobsList().size());
            System.out.println("Jobs in running state: " + jobControl.getRunningJobList().size());
            System.out.println("Jobs in success state: " + jobControl.getSuccessfulJobList().size());
            System.out.println("Jobs in failed state: " + jobControl.getFailedJobList().size());
            try {
                Thread.sleep(5000);
            } catch (Exception e) {

            }

        }
        System.exit(0);

        return job1.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(final String[] args) {
        if (args.length != 3) {
            throw new Error("Two arguments required:\n<input-dir> <output-dir> <MaxNodeNumber>");
        }
        try {
            ToolRunner.run(new TriangleCount(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }

}
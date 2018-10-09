package cse.uprm.bigdataproject1.statusbyuser;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StatusByUserDriver {
        public static void main(String[] args) throws Exception {
            if (args.length != 2) {
                System.err.println("Usage: StatusByUserDriver <input path> <output path>");
                System.exit(-1);
            }
            Job job = Job.getInstance();
            job.setJarByClass(cse.uprm.bigdataproject1.statusbyuser.StatusByUserDriver.class);
            job.setJobName("Get Status by User");

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            job.setMapperClass(cse.uprm.bigdataproject1.statusbyuser.StatusByUserMapper.class);
            job.setReducerClass(cse.uprm.bigdataproject1.statusbyuser.StatusByUserReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }

}

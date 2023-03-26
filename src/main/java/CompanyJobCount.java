import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CompanyJobCount {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "company job count");
        job.setJarByClass(CompanyJobCount.class);
        job.setMapperClass(CompanyJobCountMapper.class);
        job.setReducerClass(CompanyJobCountReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/user/hadoop/input/data.csv"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/user/hadoop/output/result"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class JobDescAnalysis {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "JobDescAnalysis");
        job.setJarByClass(JobDescAnalysis.class);

        Configuration validationConf = new Configuration(false);
        ChainMapper.addMapper(job, RatingValidationMapper.class, LongWritable.class, Text.class, LongWritable.class, Text.class, validationConf);

        Configuration analysisConf = new Configuration(false);
        ChainMapper.addMapper(job, JobDescMapper.class, LongWritable.class, Text.class, Text.class, IntWritable.class, analysisConf);

        job.setMapperClass(ChainMapper.class);

//        job.setCombinerClass(JobDescReducer.class);

        job.setReducerClass(JobDescReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        Path outPath = new Path("hdfs://localhost:9000/user/hadoop/output/jobdesc");

        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/user/hadoop/input/data.csv"));

        FileOutputFormat.setOutputPath(job, outPath);
        outPath.getFileSystem(conf).delete(outPath, true);


        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}

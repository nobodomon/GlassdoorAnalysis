import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RatingAnalysis {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "RatingAnalysis");
        job.setJarByClass(RatingAnalysis.class);

        Configuration reviewValidationConf = new Configuration(false);
        ChainMapper.addMapper(job, ReviewValidationMapper.class, LongWritable.class, Text.class, LongWritable.class, Text.class, reviewValidationConf);

        Configuration reviewAnalysisConf = new Configuration(false);
        ChainMapper.addMapper(job, RatingMapper.class, LongWritable.class, Text.class, Text.class, MapWritable.class, reviewAnalysisConf);

        job.setMapperClass(ChainMapper.class);
        //job.setCombinerClass(ReviewReducer.class);
        job.setReducerClass(RatingReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/user/hadoop/input/" + args[1]));
        Path outPath = new Path("hdfs://localhost:9000/user/hadoop/output/rating/" + args[2]);
        FileOutputFormat.setOutputPath(job, outPath);
        outPath.getFileSystem(conf).delete(outPath, true);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}

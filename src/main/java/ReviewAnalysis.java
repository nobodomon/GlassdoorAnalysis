import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ReviewAnalysis {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "ReviewAnalysis");
        job.setJarByClass(ReviewAnalysis.class);

        Configuration reviewValidationConf = new Configuration(false);
        ChainMapper.addMapper(job, ReviewValidationMapper.class, LongWritable.class, Text.class, LongWritable.class, Text.class, reviewValidationConf);

        Configuration reviewAnalysisConf = new Configuration(false);
        ChainMapper.addMapper(job, ReviewMapper.class, LongWritable.class, Text.class, Text.class, MapWritable.class, reviewAnalysisConf);

        job.setMapperClass(ChainMapper.class);
        //job.setCombinerClass(ReviewReducer.class);
        job.setReducerClass(ReviewReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/user/hadoop/input/" + args[1]));
        Path outPath = new Path("hdfs://localhost:9000/user/hadoop/output/review/" + args[2]);
        FileOutputFormat.setOutputPath(job, outPath);
        outPath.getFileSystem(conf).delete(outPath, true);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}

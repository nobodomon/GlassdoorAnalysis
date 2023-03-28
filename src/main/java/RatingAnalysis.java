import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import javax.servlet.jsp.jstl.core.Config;

public class RatingAnalysis {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "RatingAnalysis");
        job.setJarByClass(RatingAnalysis.class);

        Configuration validationConf = new Configuration(false);
        ChainMapper.addMapper(job, RatingValidationMapper.class, LongWritable.class, Text.class, LongWritable.class, Text.class, validationConf);
        Configuration analysisConf = new Configuration(false);
        ChainMapper.addMapper(job, RatingMapper.class, LongWritable.class, Text.class, Text.class, DoubleWritable.class, analysisConf);

        job.setMapperClass(ChainMapper.class);

        //job.setCombinerClass(RatingReducer.class);
        job.setReducerClass(RatingReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/user/hadoop/input/" + args[1]));
        Path outPath = new Path("hdfs://localhost:9000/user/hadoop/output/review/" + args[2]);

        FileOutputFormat.setOutputPath(job, outPath);

        outPath.getFileSystem(conf).delete(outPath, true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

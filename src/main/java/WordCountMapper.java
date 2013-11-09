import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>  {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println("key = " + key + ", value = " + value);

        for (String word : value.toString().split("\\s+")) {
            context.write(new Text(word), new IntWritable(1));
        }

        System.out.println("key = " + key + ", value = " + value);
    }

}

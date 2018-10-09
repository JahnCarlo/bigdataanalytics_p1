package cse.uprm.bigdataproject1.findreplies;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FindRepliesReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        String replies = "";
        int count = 0;

        for (Text value : values){
            replies += value.toString()+" ";
        }
        context.write(key, new Text(replies));

    }
}

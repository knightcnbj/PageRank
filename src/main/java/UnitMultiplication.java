import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.Chain;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UnitMultiplication {

    public static class TransitionMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            // input format: fromPage\t toPage1,toPage2,toPage3
            // ex: 1\t 2,3,4
            // target: build transition matrix unit -> fromPage\t toPage=probability
            // output key: from page id
            // output val: to page id = probability

            String[] parts = value.toString().trim().split("\t");

            // if toPage doesnt exist
            if (parts.length < 2)
                return;

            String fromPage = parts[0].trim();
            // if fromPage doesnt exist
            if (fromPage.equals(""))
                return;

            String[] toPages = parts[1].trim().split(",");
            int toCount = toPages.length;

            for (String toPage : toPages) {
                context.write(new Text(fromPage), new Text(toPage + "=" + (double) 1 / toCount));
            }

        }
    }

    public static class PRMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            // input format: Page\t PageRank
            // ex: 1\t 1
            // target: write to reducer
            // output key: id
            // output val: pr0

            String[] parts = value.toString().trim().split("\t");
            String page = parts[0].trim();
            String rank = parts[1].trim();
            context.write(new Text(page), new Text(rank));
        }
    }

    public static class MultiplicationReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // input key = fromPage value=<toPage=probability..., pageRank>
            // key: fromID = 1
            // value: <2=1/3, 8=1/3, 27=1/3, 1>
            // target: get the unit multiplication
            // Map<toId, prob>
            // output key: toId
            // iterate map -> pr0 * prob = output val(subPr) 

            List<String> transitionUnits = new ArrayList<String>();
            double prCell = 0;

            for (Text val : values) {
                // this part should execute n times since transition matrix is nxn
                if (val.toString().contains("="))
                    transitionUnits.add(val.toString());
                // this part should only execute once since pr matrix is nx1
                // so n different keys but each only has 1 val
                else
                    prCell = Double.parseDouble(val.toString());
            }

            for (String cell : transitionUnits) {
                // ex: cell: 2=1/3 (toId=prob)
                String[] parts = cell.split("=");
                // key: toId
                String toId = parts[0];
                double probability = Double.parseDouble(parts[1]);
                double subPr = probability * prCell
                context.write(new Text(toId), new Text(String.valueOf(subPr)));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(UnitMultiplication.class);

        //how to chain two mapper classes?
        ChainMapper.addMapper(job, TransitionMapper.class, Object.class, Text.class, Text.class, Text.class, conf);
        ChainMapper.addMapper(job, PRMapper.class, Object.class, Text.class, Text.class, Text.class, conf);

        job.setReducerClass(MultiplicationReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransitionMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PRMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }

}

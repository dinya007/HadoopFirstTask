package ru.tisov.denis.hadoop.first;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static org.apache.commons.lang.StringUtils.isEmpty;

public class ReverseSearchIndex {

    private static final String SPACE = "\\s+";
    private static final String NON_CHARACTER = "[^A-Za-z0-9 ]";
    private static final int NUM_REDUCE_TASKS = 5;
    private static final String TAB = "\\t+";

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        FileSystem fileSystem = FileSystem.newInstance(conf);

        Path firstJobInput = new Path(args[0]);
        String firstJobOutputPathName = args[1] + "-inter";
        Path firstJobOutput = new Path(firstJobOutputPathName);
        Path secondJobOutput = new Path(args[2]);

        fileSystem.deleteOnExit(firstJobOutput);
        fileSystem.delete(secondJobOutput, true);
        fileSystem.delete(firstJobOutput, true);
        fileSystem.delete(new Path(firstJobOutputPathName + "partitioner"), true);

        Job firstMapJob = initFirstJob(conf, firstJobInput, firstJobOutput);

        if (!firstMapJob.waitForCompletion(true)) System.exit(1);

        Job secondJob = initSecondJob(conf, firstJobOutput, secondJobOutput);

        secondJob.waitForCompletion(true);
    }

    private static Job initFirstJob(Configuration conf, Path mapInputPath, Path mapOutputPath) throws IOException {
        Job firstMapJob = Job.getInstance(conf, "First Map");
        firstMapJob.setJarByClass(ReverseSearchIndex.class);
        firstMapJob.setMapperClass(FirstMapper.class);
        firstMapJob.setOutputKeyClass(Text.class);
        firstMapJob.setOutputValueClass(Text.class);
        TextInputFormat.setInputPaths(firstMapJob, mapInputPath);

        firstMapJob.setNumReduceTasks(NUM_REDUCE_TASKS);
        firstMapJob.setReducerClass(FirstReducer.class);
        FileInputFormat.addInputPath(firstMapJob, mapInputPath);
        FileOutputFormat.setOutputPath(firstMapJob, mapOutputPath);
        return firstMapJob;
    }

    private static Job initSecondJob(Configuration conf, Path inputPath, Path mapOutputPath) throws IOException {
        Job job = Job.getInstance(conf, "DictionarySorter");
        job.setJarByClass(ReverseSearchIndex.class);
        job.setMapperClass(SecondMapper.class);
        job.setReducerClass(SecondReducer.class);
        job.setPartitionerClass(TotalOrderPartitioner.class);
        job.setNumReduceTasks(NUM_REDUCE_TASKS);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setSortComparatorClass(LongWritable.DecreasingComparator.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, new Path(mapOutputPath
                + ".dictionary.sorted." + getCurrentDateTime()));
        job.setPartitionerClass(TotalOrderPartitioner.class);

        Path inputDir = new Path(inputPath + "partitioner");
        Path partitionFile = new Path(inputDir, "partitioning");
        TotalOrderPartitioner.setPartitionFile(job.getConfiguration(),
                partitionFile);

        InputSampler.Sampler sampler = getSampler();
        try {
            InputSampler.writePartitionFile(job, sampler);
        } catch (ClassNotFoundException | InterruptedException e) {
            e.printStackTrace();
        }

        return job;
    }

    private static class FirstMapper extends Mapper<Object, Text, Text, Text> {

        private Text word = new Text();
        private Text id = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] articles = value.toString().split("\n");

            Set<String> uniqueWords;

            for (String article : articles) {
                String[] words = article.split(SPACE);
                id.set(words[0]);

                uniqueWords = new HashSet<>();

                for (int j = 1; j < words.length; j++) {
                    String candidateWord = words[j].replaceAll(NON_CHARACTER, "").trim();
                    if (!isEmpty(candidateWord)) uniqueWords.add(candidateWord);
                }

                for (String uniqueWord : uniqueWords) {
                    word.set(uniqueWord);
                    context.write(word, id);
                }

            }
        }
    }

    private static class FirstReducer extends Reducer<Text, Text, LongWritable, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<Text> result = new HashSet<>();
            for (Text value : values) {
                result.add(value);
            }
            context.write(new LongWritable((long) result.size()), key);
        }
    }

    private static class SecondMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] input = value.toString().split(TAB);

            if (input.length < 2) return;

            String occurrences = input[0];
            String word = input[1];
            context.write(new LongWritable(Long.parseLong(occurrences)), new Text(word));
        }

    }

    private static class SecondReducer extends Reducer<LongWritable, Text, Text, LongWritable> {
        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iterator = values.iterator();
            if (iterator.hasNext()) {
                Text next = iterator.next();
                String string = next + " " + key.toString();
                System.out.println(string);
                context.write(new Text(string), new LongWritable(0L));
            }
        }
    }

    private static String getCurrentDateTime() {
        Date d = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss");
        return sdf.format(d);
    }

    private static InputSampler.Sampler getSampler() {
        double pcnt = 10.0;
        int numSamples = NUM_REDUCE_TASKS;
        int maxSplits = NUM_REDUCE_TASKS - 1;

        return new InputSampler.RandomSampler(pcnt,
                numSamples, maxSplits);
    }

}

package com.sample;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.sample.WordCount.Map;
import com.sample.WordCount.Reduce;

public class WordCountTest {
	private Map mapper;
	private Reduce reducer;
	private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		mapper = new Map();
		mapDriver = MapDriver.newMapDriver(mapper);

		reducer = new Reduce();
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(new LongWritable(1), new Text("cat cat dog"))
				.withOutput(new Text("cat"), new IntWritable(1))
				.withOutput(new Text("cat"), new IntWritable(1))
				.withOutput(new Text("dog"), new IntWritable(1));
		mapDriver.runTest();
	}

	@Test
	public void testReducer() throws IOException {
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		reduceDriver.withInput(new Text("cat"), values).withOutput(
				new Text("cat"), new IntWritable(2));
		reduceDriver.runTest();
	}
}

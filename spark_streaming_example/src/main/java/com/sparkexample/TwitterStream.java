/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.sparkexample;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import scala.Tuple2;

import com.google.common.collect.Lists;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import twitter4j.*;

public final class TwitterStream {

	public static void main(String[] args) throws Exception {

		// twitter stuff

		HashMap<String, String> configs = new HashMap<String, String>();
		configs.put("apiKey", "BRPqNoG7WV6ONMWG9C8brmvtl");
		configs.put("apiSecret", "OZ3I4on0JiDQc7HYRsKBeevQC7lG8MBSDSi1yiserj6yTwXFAp");
		configs.put("accessToken", "722309419413991424-IBbz3hhBlroK96qkKexJaFJa3yrcKad");
		configs.put("accessTokenSecret", "UH2H02LP3wV2xw5BOPKfr3lb1rIsKFkmhN1gEqLceuoJX");

		Object[] keys = configs.keySet().toArray();
		for (int k = 0; k < keys.length; k++) {
			String key = keys[k].toString();
			String value = configs.get(key).trim();
			if (value.isEmpty()) {
				throw new Exception("Error setting authentication - value for " + key + " not set");
			}
			String fullKey = "twitter4j.oauth." + key.replace("api", "consumer");
			System.setProperty(fullKey, value);
		}

		// StreamingExamples.setStreamingLogLevels();
		SparkConf sparkConf = new SparkConf().setAppName("JavaQueueStream").setSparkHome("/root/spark/");
		sparkConf.setMaster("local[4]");
		// Create the context
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(10000));

		JavaDStream<Status> tweets = TwitterUtils.createStream(ssc);

		JavaDStream<String> statuses = tweets.map(new Function<Status, String>() {
			public String call(Status status) {
				return status.getText();
			}
		});
		statuses.print();
		ssc.start();
	}

}

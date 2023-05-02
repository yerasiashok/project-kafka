package com.twitter.twitterkafka.producer;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import lombok.Data;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@Data
public class TwitterProducer {

//    @Value("${consumerKey}")
//    private String consumerKey;
//    @Value("${consumerSecret}")
//    private String consumerSecret;
//    @Value("${token}")
//    private static String token;
//    @Value("${tokenSecret}")
//    private static String tokenSecret;

    String token= "934308982402854912-9oZrmpmkqwzbo7gRfcTyA3KV36ieIR4" ;
    String tokenSecret= "b7exMbBaAyPGiAGDSFLcX1f6z5iVYoDWXBKRX1jAmzNPn";

    String consumerKey = "p4wQEpjIt4x80toNUAR5KN1eA";
    String consumerSecret= "I1ByXPt2s7KfD7H65ZpcHxB26sfvLYs7eXIa1yZ3fNXBpfUgaZ";


    public TwitterProducer(){

    };

    public static void main(String[] args) {
        /* 3 steps
        1. We need twitter client
        2. kafka producer
        3. loop to sent tweets to kafka
        */
        new TwitterProducer().run();
    }
    public void run() {
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */

        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);

        //kafka producer
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String, String>(properties);



        //Create twitter client
        Client client = twitterClient(msgQueue);
        //Connect to twitter
        client.connect();

        //loops to send tweets to kafka
        while (!client.isDone()) {
            String msg = null;
            try{
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if(msg!=null) {
                System.out.println(msg);
                ProducerRecord<String,String> producerRecord = new ProducerRecord<>("twittertopic",msg);
            }
        }

    }

    public Client twitterClient(BlockingQueue<String> msgQueue) {



/** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.USERSTREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
// Optional: set up some followings and track terms
        //List<Long> followings = Lists.newArrayList(1234L, 566788L);
        List<String> terms = Lists.newArrayList("bitcoin");
        //.followings(followings);
        hosebirdEndpoint.trackTerms(terms);

// These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, tokenSecret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
        // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();
// Attempts to establish a connection.
        return hosebirdClient;
    }
}

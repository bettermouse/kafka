package kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by chen on 2017/8/4.
 */
public class KafkaConsumerDemo {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.83.94:9092");
        props.put("group.id", "chenhao4");
       // props.put("enable.auto.commit","true");
      //  props.put("auto.commit.interval.ms","1000000");
      //  props.put("session.timeout.ms","30000");
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset","earliest");

        KafkaConsumer<Integer,String> consumer = new KafkaConsumer(props);
     //   Map<String, List<PartitionInfo>> stringListMap = consumer.listTopics();

        consumer.assign(Arrays.asList(new TopicPartition("test",0)));
        consumer.seek(new TopicPartition("test",0),10);
      //  Set<TopicPartition> assignment = consumer.assignment();
        int ii=1;
        while(ii<100) {
            //ii++;
           // consumer.seekToEnd();
           //consumer.position()
           // ConsumerRecords<Integer, String> recordTemp = consumer.poll(0);
           // System.out.println(recordTemp.isEmpty());

            //System.out.println("重设offset");
            ConsumerRecords<Integer, String> records = consumer.poll(100);
            //
            for( ConsumerRecord<Integer, String> record:records){
                System.out.println(record.offset()+"            "+record.key()+"    "+record.value());
//                    System.out.println(record.offset());
//                    System.out.println(record.key());
//                    System.out.println(record.value());
                System.out.println("error");

            }
        }
        consumer.seek(new TopicPartition("testBug",0),0l);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //consumer.assign(Arrays.asList(new TopicPartition("test",0)));
        //consumer.assign();
        //consumer.seek();
        try{
            int i=0;
            while (i<3){
                i++;
                ConsumerRecords<Integer, String> records = consumer.poll(100);
                for( ConsumerRecord<Integer, String> record:records){
                    System.out.println(record.offset()+"            "+record.key()+"    "+record.value());
//                    System.out.println(record.offset());
//                    System.out.println(record.key());
//                    System.out.println(record.value());
                }
          //      consumer.seek(new TopicPartition("test",0),0l);
            }

        }finally {
            consumer.close();
        }
    }
}

package kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Created by chen on 2017/7/26.
 */
public class ProducerDemo {
    public static void main(String[] args) {
        boolean isAsync = args.length == 0 || !args[0].trim().equalsIgnoreCase("sync");
        Properties props = new Properties();
        //服务端主机名,端口号
        props.put("bootstrap.servers", "localhost:9092");
        props.put("client.id", "DemoProducer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("compression.type","gzip");
        //key value都是字节数组,要配置序列化器
        KafkaProducer producer = new KafkaProducer<>(props);
       // List test = producer.partitionsFor("test");
        //String topic = "testBug";
        String topic = "testLog";
        int messageNo = 1;
        long start = System.currentTimeMillis();
        while (true) {
            String messageStr = "Message_" + messageNo;
            long startTime = System.currentTimeMillis();
           if (isAsync) {
               //第一个参数是ProducerRecord对象,封装了 topic key value,第二个是callback对象,收到kafka发来的ack，会调 onCompletion方法
               System.out.println("key"+ messageNo +"  value"+messageStr);
               Future send = producer.send(new ProducerRecord<>(topic, messageNo, messageStr), new DemoCallBack(startTime, messageNo, messageStr));
           } else {
               Future<RecordMetadata> send = producer.send(new ProducerRecord<>(topic, messageNo, messageStr));
               try {
                   RecordMetadata recordMetadata = send.get();
                   System.out.println("异步消息"+messageNo + "--" + messageStr + "sent to partition" + recordMetadata.partition() + "--" + recordMetadata.offset() + "--");

               } catch (InterruptedException e) {
                   e.printStackTrace();
               } catch (ExecutionException e) {
                   e.printStackTrace();
               }
           }
            messageNo++; //递增消息的key

        }

    }
}


class DemoCallBack implements Callback {  //回调对象
    private final long startTime;    //开始发送消息的时间戳
    private final int key;           //消息的key
    private final String message;    //消费的value

    public DemoCallBack(long startTime, int key, String message) {
        this.startTime = startTime;
        this.key = key;
        this.message = message;
    }


    /**
     * 收到ACK后,会回调此函数
     *
     * @param recordMetadata 失败null
     * @param e              成功null
     */
    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (recordMetadata != null) {
            // RecordMetadata包含了分区信息 offset信息等等
            System.out.println(key + "--" + message + "sent to partition" + recordMetadata.partition() + "--" + recordMetadata.offset() + "--" + elapsedTime);
        } else {
            e.printStackTrace();
        }
    }
}
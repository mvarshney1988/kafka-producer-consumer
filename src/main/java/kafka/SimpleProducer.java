package kafka;

//import util.properties packages
import java.util.Properties;

//import KafkaProducer packages
import org.apache.kafka.clients.producer.KafkaProducer;
//import simple producer packages
import org.apache.kafka.clients.producer.Producer;
//import ProducerRecord packages
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
//Create java class named "SimpleProducer”
public class SimpleProducer {
public static void main(String[] args) throws Exception{
   // Check arguments length value
//   if(args.length == 0){
//      System.out.println("Enter topic name");
//      return;
//   }
   //Assign topicName to string variable
   String topicName = "demo";
   // create instance for properties to access producer configs
   Properties props = new Properties();
   ClassLoader original = Thread.currentThread().getContextClassLoader();
   Thread.currentThread().setContextClassLoader(null);
   //Assign localhost id

props.put("key.serializer", Class.forName("org.apache.kafka.common.serialization.StringSerializer"));
props.put("value.serializer", Class.forName("org.apache.kafka.common.serialization.StringSerializer"));
   props.put("bootstrap.servers", "localhost:9091,localhost:9092,localhost:9093");
   //Set acknowledgements for producer requests.
   props.put("acks", "all");
   //If the request fails, the producer can automatically retry,
   props.put("retries", 1);
   //Specify buffer size in config
   props.put("batch.size", 16384);
   //Reduce the no of requests less than 0
   props.put("linger.ms", 1);
   //The buffer.memory controls the total amount of memory available to the producer for buffering.
   props.put("buffer.memory", 33554432);
   props.put("transactional.id", "my-transactional-45");
   props.put("key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer");
   props.put("value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer");
   Producer<String, String> producer = new KafkaProducer
      <String, String>(props);
   
   producer.initTransactions();

   try {
       producer.beginTransaction();
       for (int i = 0; i < 100; i++)
           producer.send(new ProducerRecord<String, String>(topicName, Integer.toString(i), Integer.toString(i)));
       
       producer.commitTransaction();
       System.out.println("Message sent successfully");
   } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
       // We can't recover from these exceptions, so our only option is to close the producer and exit.
       producer.close();
   } catch (KafkaException e) {
       // For all other exceptions, just abort the transaction and try again.
       producer.abortTransaction();
   }
   producer.close();
//   for(int i = 0; i < 10; i++)
//      producer.send(new ProducerRecord<String, String>(topicName,
//         Integer.toString(i), Integer.toString(i)));
//            System.out.println("Message sent successfully");
//            producer.close();
//}
}
}


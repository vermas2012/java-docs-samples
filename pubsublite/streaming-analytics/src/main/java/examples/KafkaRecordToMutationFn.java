package examples;

import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.Mutation.SetCell;
import com.google.bigtable.v2.MutationOrBuilder;
import com.google.cloud.ByteArray;
import com.google.cloud.pubsublite.proto.SequencedMessage;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaRecordToMutationFn extends DoFn<SequencedMessage, KV<ByteString, Iterable<Mutation>>>{

  private static final Logger LOG = LoggerFactory.getLogger(KafkaRecordToMutationFn.class);

  @ProcessElement
  public void processElement(ProcessContext context) throws Exception {
    SequencedMessage message = context.element();

    byte[] rawMessage = message.getMessage().getData().toByteArray();
    String messageString = new String(rawMessage);
    LOG.info("### Processing message:" + new String(rawMessage) );
    if(messageString.equals("test-val-constrcutor")){
      LOG.info("### skipping message test-val-constrcutor" );
      return;
    }
    JsonConverter jsonConverter = new JsonConverter();
    Map<String, String> jsonConvertorConfig = new HashMap<>();
    jsonConvertorConfig.put("converter.type", "value");
    jsonConverter.configure(new HashMap<>(), false);
    try{

    SchemaAndValue schemaAndValue = jsonConverter.toConnectData("shitanshu-test", rawMessage);

    LOG.error("### Deserialized the schema and value: " + schemaAndValue.toString());

    Struct  struct = (Struct) schemaAndValue.value();

    LOG.error("### Deserialized Struct.After: " + struct.getStruct("after").toString());

    Mutation.Builder builder = Mutation.newBuilder();
    SetCell.Builder scBuilder = builder.getSetCellBuilder();
    scBuilder.setFamilyName("cf").setColumnQualifier(ByteString.copyFromUtf8("cassandra-kafka-test")).setTimestampMicros(System.currentTimeMillis() * 1000).setValue(ByteString.copyFromUtf8("testValue"));
    Mutation mutation = builder.setSetCell(scBuilder.build()).build();
    context.output(KV.of(ByteString.copyFromUtf8("Shitanshu-test-key"), Arrays.asList(mutation)));
    } catch(RuntimeException e){
      LOG.error("Failed to process message " + messageString, e);
      throw e;
    }
  }
}

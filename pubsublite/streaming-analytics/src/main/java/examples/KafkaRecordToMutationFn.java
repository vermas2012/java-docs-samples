package examples;

import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.Mutation.SetCell;
import com.google.bigtable.v2.MutationOrBuilder;
import com.google.cloud.ByteArray;
import com.google.cloud.pubsublite.proto.SequencedMessage;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaRecordToMutationFn extends DoFn<SequencedMessage, KV<ByteString, Iterable<Mutation>>>{

  private static final Logger LOG = LoggerFactory.getLogger(KafkaRecordToMutationFn.class);

  static final String AFTER = "after";
  static final String OPERATION = "op";
  static final String SOURCE = "source";
  static final String TIMESTAMP = "ts_ms";


  public enum Operation {
    INSERT("i"),
    UPDATE("u"),
    DELETE("d"),
    RANGE_TOMBSTONE("r");

    private String value;

    Operation(String value) {
      this.value = value;
    }

    public static Operation fromValue(String text) {
      for (Operation op : Operation.values()) {
        if (op.value.equalsIgnoreCase(text)) {
          return op;
        }
      }
      return null;
    }

    public String getValue() {
      return value;
    }

    @Override
    public String toString() {
      return value;
    }
  }

  @ProcessElement
  public void processElement(ProcessContext context) throws Exception {
    SequencedMessage message = context.element();

    byte[] rawMessage = message.getMessage().getData().toByteArray();
    String messageString = new String(rawMessage);
    LOG.info("### Processing message:" + new String(rawMessage) );
    if(messageString.equals("test-val-constrcutor")){
      return;
    }
    JsonConverter jsonConverter = new JsonConverter();
    Map<String, String> jsonConvertorConfig = new HashMap<>();
    jsonConvertorConfig.put("converter.type", "value");
    jsonConverter.configure(new HashMap<>(), false);
    try{

    SchemaAndValue schemaAndValue = jsonConverter.toConnectData("shitanshu-test", rawMessage);

    Struct  struct = (Struct) schemaAndValue.value();
    // Not useful right now.
    Schema schema = schemaAndValue.schema();
    Struct source = struct.getStruct("source");
    long timestamp = source.getInt64(TIMESTAMP);
    Struct after = struct.getStruct(AFTER);
    Operation op = Operation.fromValue(struct.getString(OPERATION));

    Schema afterSchema = after.schema();
    // LOG.error("Field schema for user_id.value: " + afterSchema.field("user_id").schema().field("value").toString());
    String userId = after.getStruct("user_id").getInt32("value") + "";

    Mutation.Builder builder = Mutation.newBuilder();
    SetCell.Builder scBuilder = builder.getSetCellBuilder();
    scBuilder.setFamilyName("cf").setColumnQualifier(ByteString.copyFromUtf8("cassandra-kafka-test")).setTimestampMicros(timestamp * 1000).
        setValue(ByteString.copyFromUtf8(after.toString()));
    Mutation mutation = builder.setSetCell(scBuilder.build()).build();
    context.output(KV.of(ByteString.copyFrom(userId.getBytes()), Arrays.asList(mutation)));
    } catch(RuntimeException e){
      LOG.error("Failed to process message " + messageString, e);
      throw e;
    }
  }
}

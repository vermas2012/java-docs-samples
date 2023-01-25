package examples;

import com.google.bigtable.v2.Mutation;
import com.google.cloud.pubsublite.proto.PubSubMessage;
import com.google.cloud.pubsublite.proto.SequencedMessage;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import junit.framework.TestCase;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class KafkaRecordToMutationFnTest extends TestCase {

  @Rule
  public final transient TestPipeline p = TestPipeline.create();


  @Test
  public void test1(){
    String messageJson = "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"int64\",\"optional\":false,\"field\":\"ts_ms\"},{\"type\":\"string\",\"optional\":false,\"field\":\"op\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"version\"},{\"type\":\"string\",\"optional\":false,\"field\":\"connector\"},{\"type\":\"string\",\"optional\":false,\"field\":\"name\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"ts_ms\"},{\"type\":\"string\",\"optional\":true,\"name\":\"io.debezium.data.Enum\",\"version\":1,\"parameters\":{\"allowed\":\"true,last,false,incremental\"},\"default\":\"false\",\"field\":\"snapshot\"},{\"type\":\"string\",\"optional\":false,\"field\":\"db\"},{\"type\":\"string\",\"optional\":true,\"field\":\"sequence\"},{\"type\":\"string\",\"optional\":false,\"field\":\"cluster\"},{\"type\":\"string\",\"optional\":false,\"field\":\"file\"},{\"type\":\"int32\",\"optional\":false,\"field\":\"pos\"},{\"type\":\"string\",\"optional\":false,\"field\":\"keyspace\"},{\"type\":\"string\",\"optional\":false,\"field\":\"table\"}],\"optional\":false,\"name\":\"source\",\"field\":\"source\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"struct\",\"fields\":[{\"type\":\"int32\",\"optional\":true,\"field\":\"value\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"deletion_ts\"},{\"type\":\"boolean\",\"optional\":false,\"field\":\"set\"}],\"optional\":true,\"name\":\"cell_value\",\"version\":1,\"field\":\"user_id\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":true,\"field\":\"value\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"deletion_ts\"},{\"type\":\"boolean\",\"optional\":false,\"field\":\"set\"}],\"optional\":true,\"name\":\"cell_value\",\"version\":1,\"field\":\"user_name\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"method\"},{\"type\":\"array\",\"items\":{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"name\"},{\"type\":\"string\",\"optional\":false,\"field\":\"value\"},{\"type\":\"string\",\"optional\":false,\"field\":\"type\"}],\"optional\":false,\"name\":\"clustering_value\",\"version\":1},\"optional\":false,\"name\":\"clustering_values\",\"version\":1,\"field\":\"values\"}],\"optional\":true,\"name\":\"_range_start\",\"version\":1,\"field\":\"_range_start\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"method\"},{\"type\":\"array\",\"items\":{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"name\"},{\"type\":\"string\",\"optional\":false,\"field\":\"value\"},{\"type\":\"string\",\"optional\":false,\"field\":\"type\"}],\"optional\":false,\"name\":\"clustering_value\",\"version\":1},\"optional\":false,\"name\":\"clustering_values\",\"version\":1,\"field\":\"values\"}],\"optional\":true,\"name\":\"_range_end\",\"version\":1,\"field\":\"_range_end\"}],\"optional\":false,\"name\":\"after\",\"version\":1,\"field\":\"after\"}],\"optional\":false,\"name\":\"io.debezium.connector.cassandra.test_prefix.default.test.Envelope\"},\"payload\":{\"ts_ms\":1674682901087,\"op\":\"i\",\"source\":{\"version\":\"2.2.0-SNAPSHOT\",\"connector\":\"cassandra\",\"name\":\"test_prefix\",\"ts_ms\":1674593480556,\"snapshot\":\"false\",\"db\":\"NULL\",\"sequence\":null,\"cluster\":\"shitashu-cassandra-test\",\"file\":\"CommitLog-7-1673675172686.log\",\"pos\":33244260,\"keyspace\":\"default\",\"table\":\"test\"},\"after\":{\"user_id\":{\"value\":70647064,\"deletion_ts\":null,\"set\":true},\"user_name\":{\"value\":\"this-is-a-very-large-value-cassandra-val-########################-7064\",\"deletion_ts\":null,\"set\":true},\"_range_start\":null,\"_range_end\":null}}}";
    PubSubMessage message = PubSubMessage.newBuilder().setData(ByteString.copyFromUtf8(messageJson)).build();
    SequencedMessage sequencedMessage = SequencedMessage.newBuilder().setMessage(message).build();
    PCollection<SequencedMessage> input =
        p.apply(Create.of(sequencedMessage));

    PCollection<KV<ByteString, Iterable<Mutation>>> output = input.apply(ParDo.of(new KafkaRecordToMutationFn()));

    p.run();

  }

}
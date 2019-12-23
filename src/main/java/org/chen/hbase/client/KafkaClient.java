package org.chen.hbase.client;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.chen.hbase.Delete;
import org.chen.hbase.Put;
import org.chen.hbase.WALEdit;
import org.chen.hbase.util.KafkaUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.PooledByteBufAllocator;

public class KafkaClient extends AbstractHBaseClient {
  private static final Log LOG = LogFactory.getLog(KafkaClient.class);
  private final Producer<ByteBuffer, ByteBuffer> producer;

  KafkaClient() throws IOException {
    super();
    if (conf.get("kafka.bootstrap.servers") == null) {
      throw new IOException("kafkaServers can't be null.");
    }
    Properties props = new Properties();
    props.put("bootstrap.servers", conf.get("kafka.bootstrap.servers"));
    props.put("acks", conf.get("kafka.acks", "all"));
    props.put("delivery.timeout.ms", conf.get("kafka.delivery.timeout.ms", "35000"));
    props.put("request.timeout.ms", conf.get("kafka.request.timeout.ms", "15000"));
    props.put("key.serializer", "org.apache.kafka.common.serialization.ByteBufferSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteBufferSerializer");
    producer = new KafkaProducer<>(props);
  }

  private void mutate(String tableName, Mutation mutation) throws IOException {
    initTable(tableName);
    int partitionCount = this.tables.get(tableName).getPartitions();
    String topic = KafkaUtil.getTableTopic(tableName);
    WALEdit value = buildWALEdit(mutation);
    long valueSize = value.heapSize();
    ByteBuf bb = PooledByteBufAllocator.DEFAULT.buffer((int) valueSize);
    try (ByteBufOutputStream bbos = new ByteBufOutputStream(bb)) {
      value.write(bbos);
      ProducerRecord<ByteBuffer, ByteBuffer> record = new ProducerRecord<>(
          topic, // topic
          KafkaUtil.getTablePartition(Bytes.toString(mutation.getRow()), partitionCount),
          org.apache.kafka.common.utils.Bytes.EMPTY_BUFFER, // now WALKey needed now
          bb.nioBuffer());
      Future<RecordMetadata> res = producer.send(record);
      producer.flush();
      res.get();
    } catch (Exception e) {
      exceptionCallback(tableName, e);
    } finally {
      bb.release();
    }
  }

  public void put(String tableName, Put put) throws IOException {
    mutate(tableName, put);
  }

  public void putAsync(String tableName, Put put) throws IOException {
    initTable(tableName);
    int partitionCount = this.tables.get(tableName).getPartitions();
    String topic = KafkaUtil.getTableTopic(tableName);
    WALEdit value = buildWALEdit(put);
    int valueSize = (int) value.heapSize();
    ByteBuf bb = PooledByteBufAllocator.DEFAULT.buffer(valueSize);
    try (ByteBufOutputStream bbos = new ByteBufOutputStream(bb)) {
      value.write(bbos);
      ProducerRecord<ByteBuffer, ByteBuffer> record = new ProducerRecord<>(
          topic, // topic
          KafkaUtil.getTablePartition(Bytes.toString(put.getRow()), partitionCount),
          org.apache.kafka.common.utils.Bytes.EMPTY_BUFFER, // now WALKey needed now
          bb.nioBuffer());
      producer.send(record, (m,e) -> {
        bb.release();
        if (e != null) {
          LOG.error(e.getMessage(), e);
        }
      });
    } catch (IOException e) {
      exceptionCallback(tableName, e);
    }
  }

  private void mutate(String tableName, List<? extends Mutation> mutations, boolean async) throws IOException {
    if (mutations == null || mutations.size() == 0) {
      return;
    }
    initTable(tableName);
    int partitionCount = this.tables.get(tableName).getPartitions();
    String topic = KafkaUtil.getTableTopic(tableName);
    try {
      Future<RecordMetadata> last = null;
      for (int i = 0; i < mutations.size(); i++) {
        Mutation mutation = mutations.get(i);
        WALEdit value = buildWALEdit(mutation);
        int valueSize = (int) value.heapSize();
        ByteBuf bb = PooledByteBufAllocator.DEFAULT.buffer(valueSize);
        try (ByteBufOutputStream bbos = new ByteBufOutputStream(bb)) {
          value.write(bbos);
          ProducerRecord<ByteBuffer, ByteBuffer> record = new ProducerRecord<>(
              topic, // topic
              KafkaUtil.getTablePartition(Bytes.toString(mutation.getRow()), partitionCount),
              org.apache.kafka.common.utils.Bytes.EMPTY_BUFFER, // now WALKey needed now
              bb.nioBuffer());
          Future<RecordMetadata> res = producer.send(record, (m,e) -> {
            bb.release();
            if (e != null) {
              LOG.error(e.getMessage(), e);
            }
          });
          if (i == mutation.size() - 1) {
            last = res;
          }
        }
      }
      if (!async) {
        producer.flush();
        if (last != null) {
          last.get();
        }
      }
    } catch (Exception e) {
      exceptionCallback(tableName, e);
    }
  }

  @Override
  public void put(String tableName, List<Put> puts) throws IOException {
    mutate(tableName, puts, false);
  }

  @Override
  public void putAsync(String tableName, List<Put> puts) throws IOException {
    mutate(tableName, puts, true);
  }

  private WALEdit buildWALEdit(Mutation mutation) {
    WALEdit edit = new WALEdit();
    for (List<Cell> cells : mutation.getFamilyCellMap().values()) {
      for (Cell cell : cells) {
        edit.add(cell);
      }
    }
    return edit;
  }

  @Override
  public void close() {
    producer.close();
  }

  @Override
  public void flushCommits(String tableName) throws IOException {
    producer.flush();	
  }

  @Override
  public void delete(String tableName, Delete delete) throws IOException {
    mutate(tableName, delete);
  }

  @Override
  public void delete(String tableName, List<Delete> deletes) throws IOException {
	mutate(tableName, deletes, false);
  }
}

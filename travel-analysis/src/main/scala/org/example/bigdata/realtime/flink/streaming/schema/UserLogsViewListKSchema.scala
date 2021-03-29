package org.example.bigdata.realtime.flink.streaming.schema

import com.google.gson.Gson
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.connectors.kafka.{KafkaDeserializationSchema, KafkaSerializationSchema}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.example.bigdata.realtime.flink.streaming.rdo.QRealtimeDO.UserLogViewListData
import org.example.bigdata.realtime.utils.{CommonUtil, JsonUtil}

import java.lang

class UserLogsViewListKSchema(topic:String) extends KafkaSerializationSchema[UserLogViewListData]
  with KafkaDeserializationSchema[UserLogViewListData] {

  /**
   * 反序列化
   * @return
   */
  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): UserLogViewListData = {
    val key = record.key()
    val value = record.value()
    val gson : Gson = new Gson()
    val log :UserLogViewListData = gson.fromJson(new String(value), classOf[UserLogViewListData])
    log
  }

  /**
   * 序列化
   * @param element
   * @return
   */
  override def serialize(element: UserLogViewListData, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    val sid = element.sid
    val userDevice = element.userDevice
    val userID = element.userID
    val tmp = sid + userDevice+ userID
    val key = CommonUtil.getMD5AsHex(tmp.getBytes)

    val value = JsonUtil.gObject2Json(element)
    new ProducerRecord[Array[Byte], Array[Byte]](topic, key.getBytes, value.getBytes)
  }

  override def isEndOfStream(nextElement: UserLogViewListData): Boolean = {
    return false
  }

  override def getProducedType: TypeInformation[UserLogViewListData] = {
    return TypeInformation.of(classOf[UserLogViewListData])
  }
}

package org.example.bigdata.realtime.flink.streaming.funs.cep

import org.apache.flink.cep.functions.PatternProcessFunction
import org.apache.flink.util.Collector
import org.example.bigdata.realtime.flink.constant.QRealTimeConstant
import org.example.bigdata.realtime.flink.streaming.rdo.QRealtimeDO.{UserLogPageViewAlertData, UserLogPageViewData}
import scala.collection.JavaConversions._
/**
 * 用户行为日志涉及的复杂事件处理
 */
object UserLogsCepFun {

  /**
   * 页面浏览模式匹配处理函数
   */
  class UserLogsViewPatternProcessFun extends PatternProcessFunction[UserLogPageViewData,UserLogPageViewAlertData]{
    override def processMatch(`match`: java.util.Map[String, java.util.List[UserLogPageViewData]],
                              ctx: PatternProcessFunction.Context,
                              out: Collector[UserLogPageViewAlertData]): Unit = {
      val datas :java.util.List[UserLogPageViewData] = `match`.getOrDefault(QRealTimeConstant.FLINK_CEP_VIEW_BEGIN, new java.util.ArrayList[UserLogPageViewData]())
      if(!datas.isEmpty){
        //需要引入scala和java互转
        for(data <- datas.iterator()){
          val viewAlertData = UserLogPageViewAlertData(data.userDevice, data.userId, data.userRegion,
            data.userRegionIP, data.duration, data.ct)
          out.collect(viewAlertData)
        }
      }
    }
  }
}

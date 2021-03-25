package org.example.bigdata.realtime.flink.streaming.rdo.typeinfomation

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}

object QRealTimeDimTypeInformation {
  //旅游产品表涉及列类型(所选列集合)
  //注意导入包名为：org.apache.flink.api.common.typeinfo..
  def getProductDimFieldTypeInfos() : List[TypeInformation[_]] = {
    var colTypeInfos :List[TypeInformation[_]] = List[TypeInformation[_]]()
    colTypeInfos = colTypeInfos.:+(BasicTypeInfo.STRING_TYPE_INFO)
    colTypeInfos = colTypeInfos.:+(BasicTypeInfo.INT_TYPE_INFO)
    colTypeInfos = colTypeInfos.:+(BasicTypeInfo.STRING_TYPE_INFO)
    colTypeInfos = colTypeInfos.:+(BasicTypeInfo.STRING_TYPE_INFO)
    colTypeInfos = colTypeInfos.:+(BasicTypeInfo.STRING_TYPE_INFO)
    colTypeInfos = colTypeInfos.:+(BasicTypeInfo.STRING_TYPE_INFO)
    colTypeInfos
  }
}

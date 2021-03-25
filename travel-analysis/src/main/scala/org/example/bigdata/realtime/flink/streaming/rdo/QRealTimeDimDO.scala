package org.example.bigdata.realtime.flink.streaming.rdo

object QRealTimeDimDO {
    /**
    - 旅游产品维度数据
     */
    /**
    - 旅游产品维度数据
    */
    case class ProductDimDO(productID:String, productLevel:Int,
                            productType:String,
                            depCode:String, desCode:String, toursimType:String)

}

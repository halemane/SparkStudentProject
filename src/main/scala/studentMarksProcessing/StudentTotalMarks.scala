package studentMarksProcessing

import dataHandler.StudenMarksRecParser
import dataModeler.StudentMarksSchema
import org.apache.spark.{SparkConf, SparkContext}

object StudentTotalMarks {
  def main(args: Array[String]): Unit = {
    val conf= new SparkConf().setAppName(getClass.getName).setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val rawMarksData= sc.textFile("src/main/resources/student_marks.csv")
    val rawStdData= scala.io.Source.fromFile("src/main/resources/student_data.csv").getLines()
    val parsedData= rawMarksData.map(line=> {
      val record = StudenMarksRecParser.marksRecParse(line)
      if (record.isRight)
        (true, record.right.get)
      else
        (false, line)
    })
    val normalRec= parsedData.filter(t=> t._1==true).map(t=> t._2 match{
      case x:StudentMarksSchema=> x
    })
    val marksData= normalRec.map(obj=> (obj.stdId,(obj.marks,obj.std)))

    val stdData=rawStdData.map(line=>line.split(",")).map(arr=>(arr(0).toInt,(arr(1),arr(2)))).toMap
    val broadCastMap=sc.broadcast(stdData)
    val totalMarks= marksData.reduceByKey((x,y)=>(x._1+y._1,x._2))
    val totalMarksStdInfo= totalMarks.map(t=>{
      val stdId=t._1
      val stdInfo=broadCastMap.value.getOrElse(stdId,("unknown","unknown"))
      (stdInfo,t)
    })
    val finalRDD= totalMarksStdInfo.map(t=> (t._2._1,t._1._1,t._1._2,t._2._2._1,t._2._2._2)).sortBy(t=> -t._4)

    finalRDD.foreach(x=>println(x))


  }

}

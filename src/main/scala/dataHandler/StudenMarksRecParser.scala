package dataHandler

import dataModeler.StudentMarksSchema

object StudenMarksRecParser {
  def marksRecParse(rec:String):Either[Exception,StudentMarksSchema]={
  val splitArr= rec.split(",")
    if(splitArr.size==4)
      {
        val smo=StudentMarksSchema(splitArr(0).toInt,splitArr(1),splitArr(2).toInt,splitArr(3))
        val r= Right(smo)
        r
      }
    else
      Left(new Exception)
  }
}

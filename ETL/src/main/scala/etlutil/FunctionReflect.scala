package etlutil

/**
  * Created by SNOW on 2017/5/17.
  */
object FunctionReflect {
  //val loader: ClassLoader = ClassLoader.getSystemClassLoader
  val loader: ClassLoader = Thread.currentThread.getContextClassLoader
  val clazz: Class[_] = loader.loadClass("etlutil.GeekUtils")
  def callFunction(functionName:String, parameter1:String, parameter2:String):String={
    //val dateFormat = clazz.getDeclaredMethod("dateFormatToString", classOf[String], classOf[String])
    if(parameter2 == null || parameter2.equalsIgnoreCase("")){
      callFunction(functionName, parameter1)
    }else{
      val function = clazz.getDeclaredMethod(functionName, classOf[String], classOf[String])
      function.invoke(clazz,parameter1,parameter2).toString
    }
  }
  def callFunction(functionName:String, parameter1:String):String={
    if(parameter1 == null || parameter1.equalsIgnoreCase("")){
      callFunction(functionName)
    }else{
      val function = clazz.getDeclaredMethod(functionName, classOf[String])
      function.invoke(clazz,parameter1).toString
    }
  }
  def callFunction(functionName:String):String={
    val function = clazz.getDeclaredMethod(functionName)
    function.invoke(clazz).toString
  }
}

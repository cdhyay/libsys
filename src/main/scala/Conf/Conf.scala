package Conf

object Conf {
  /**
    * 论文编号偏移量
    */
  val paperIdOffset: Int = 1000000000

  /**
    * 任务数量
    */
  val numTasks: Int = 6

  /**
    * 分区数量
    */
  val numPartitions: Int = 2

  /**
    * 精确性权重
    */
  val weight1: (Int, Int, Int, Int, Int, Int, Int, Int) = (30, 1, 90, 0, 3, 280, 20, 50)

  /**
    * 折中权重
    */
  val weight2: (Int, Int, Int, Int, Int, Int, Int, Int) = (20, 2, 60, 0, 6, 200, 40, 100)

  /**
    * 多样性权重
    */
  val weight3: (Int, Int, Int, Int, Int, Int, Int, Int) = (20, 10, 60, 0, 30, 200, 200, 200)
}


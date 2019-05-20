import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


object ScalaApp{

	def main(args:Array[String]){
		val sc = new SparkContext("local[2]","First Spark App")
		//将CSV文件转换为(user,product,price)的文件
		val data = sc.textFile("learngit/spark_test/data/UserPurchaseHistory.csv")
		               .map(line => line.split(","))
		               .map(purchaseRecord => (purchaseRecord(0),purchaseRecord(1),purchaseRecord(2)))
		//求购买次数
		val numPurchases = data.count()
		//求有多少个不同客户购买商品
		val uniqueUsers = data.map{case (user,product,price) => user}.distinct().count()
		//求和得出总收入
		val totalRevenue = data.map{case (user,product,price) => price.toDouble}.sum()
		//求最畅销的产品是什么
		val productByPopularity = data.map{case (user,product,price) => (product,1)}.reduceByKey(_+_).collect().sortBy(-_._2)
		val mostPopular = productByPopularity(0)

	}
}
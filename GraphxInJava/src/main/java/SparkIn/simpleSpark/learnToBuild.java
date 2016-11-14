package SparkIn.simpleSpark;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.EdgeContext;
import org.apache.spark.graphx.EdgeRDD;
import org.apache.spark.graphx.EdgeTriplet;
import org.apache.spark.graphx.TripletFields;
import org.apache.spark.graphx.VertexRDD;
import org.apache.spark.graphx.impl.GraphImpl;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.AbstractFunction3;
import scala.runtime.BoxedUnit;

/*
 * 属性图已学会的技能;
 * 1.会生成自定义的VertexRDD以及EdgeRDD（Vertex以及Edge的property是自定义的类型）
 * 2.会利用VertexRDD以及EdgeRDD来构建Graph
 * 3.会利用foreach以及lambda表达式输出RDD中的元素
 * 4.会传递函数给filter函数作为参数，利用filter（）函数筛选出RDD中满足条件的元素，构成新的RDD数据集
 * 5.会构建EdgeTriplet，将一条边和两端的顶点连接起来，构成一个JavaRDD<String>
 * 6.会操作subgraph函数生成子图，提供限制vertex 以及 edge集合的条件来构建新的图
 * 7.会操作图的结构函数reverse
 * 8.会从edge-list文件调用GraphLoader函数构建图，默认的边和点的属性值为1 
 * 9.会调用PageRank函数，并找出rank最大的点
 * 10.innerjoin Vertex实现
 */

public class learnToBuild {

	static abstract class SerializableFunction1<T1, R> extends AbstractFunction1<T1, R> 
			implements Serializable {

	}

	static abstract class SerializableFunction2<T1, T2, R> extends AbstractFunction2<T1, T2, R>
			implements Serializable {

	}

	static abstract class SerializableFunction3<T1, T2, T3, R> extends AbstractFunction3<T1, T2, T3, R>
	        implements Serializable{

		public VertexProperty apply(Object a, msg b, UserProperty c, VertexProperty d) {
			// TODO Auto-generated method stub
			return null;
		}
		
	}
	
	static class VertexProperty  implements Serializable{

		private String name;
		private Integer num;
		
		public VertexProperty(String name, Integer num) {
			this.name= name;
			this.num = num;
		}
		
		public String getName() {
			return name;
		}
	
		public Integer getNum() {
		return num;
	}

		public void setName(String name) {
			this.name = name;
		}
		public void setNum(Integer num) {
			this.num = num;
		}
		
		public String toString(){
			return "(" + this.name + "," + this.num + ")";
		}
	}

	static class UserProperty implements Serializable {
		private String name;
		private String title;
		private int age;
		{

		}

		
		public UserProperty(String name, String title, int age) {
			this.name = name;
			this.title = title;
			this.age = age;
		}

		public String getName() {
			return name;
		}

		public String getTitle() {
			return title;
		}

		public int getAge() {
			return age;
		}

		public void setAge(int age) {
			this.age = age;
		}

		public String toString() {
			return "(" + this.name + "," + this.title + "," + this.age + ")";
		}
	}

//	class ProductProperty extends VertexProperty {
//		private String name;
//		private Double price;
//
//		public ProductProperty(String name, Double price) {
//			this.name = name;
//			this.price = price;
//			// TODO Auto-generated constructor stub
//		}
//
//		public String getName() {
//			return name;
//		}
//
//		public Double getPrice() {
//			return price;
//		}
//	}

	// org.apache.spark.graphx.Graph<VertexProperty, String> myGraph = null;

	

	private static final ClassTag<Integer> tagInteger = ClassTag$.MODULE$.apply(Integer.class);
	private static final ClassTag<String> tagString = ClassTag$.MODULE$.apply(String.class);
	private static final ClassTag<UserProperty> tagUser = ClassTag$.MODULE$.apply(UserProperty.class);
	private static final ClassTag<msg> tagMsg = ClassTag$.MODULE$.apply(msg.class);
	private static final ClassTag<UserProperty> tagNewUser = ClassTag$.MODULE$.apply(VertexProperty.class);
	private static final ClassTag<VertexProperty> tagJoinUser = ClassTag$.MODULE$.apply(VertexProperty.class);
	//public static final scala.Option<UserProperty> optionUser = scala.Option$.MODULE$.apply(new UserProperty());
    
	// private static final scala.Predef.$eq$colon$eq<UserProperty,
	// VertexProperty> preChange = scala.Predef$.MODULE$.;
	static class msg implements Serializable {
		private int num;
		private int age;

		public msg(int num, int age) {
			this.num = num;
			this.age = age;
			// TODO Auto-generated constructor stub
		}

		public int getAge() {
			return age;
		}

		public int getNum() {
			return num;
		}

		public void setAge(int age) {
			this.age = age;
		}

		public String toString() {
			return "(" + this.num + "," + this.age + ")";
		}
	}

	private static final SerializableFunction1<EdgeContext<UserProperty, String, msg>, BoxedUnit> sendMsg = new SerializableFunction1<EdgeContext<UserProperty, String, msg>, BoxedUnit>() {
		public BoxedUnit apply(EdgeContext<UserProperty, String, msg> ec) {
			// ec
			// ec.sendToDst(ec.srcAttr() + 1);
			// return BoxedUnit.UNIT;
			if (ec.dstAttr().getAge() < ec.srcAttr().getAge()) {
				ec.sendToDst(new msg(1, ec.srcAttr().getAge()));

			}
			return BoxedUnit.UNIT;
		}
	};
	// mergeMsg
	private static final SerializableFunction2<msg, msg, msg> mergeMsg = new SerializableFunction2<msg, msg, msg>() {
		public msg apply(msg a, msg b) {
			return new msg(a.getNum() + b.getNum(), a.getAge() + b.getAge());

		}
	};

	public static void main(String[] args) {
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setMaster("local").setAppName("EdgeCount"));
		JavaRDD<Tuple2<Object, UserProperty>> myVertices = sc
				.parallelize(Arrays.asList(new Tuple2<Object, UserProperty>(1L, new UserProperty("rxin", "intern", 23)),
						new Tuple2<Object, UserProperty>(2L, new UserProperty("Tin", "intern", 26)),
						new Tuple2<Object, UserProperty>(3L, new UserProperty("franklin", "intern", 24)),
						new Tuple2<Object, UserProperty>(5L, new UserProperty("tommy", "intern", 22)),
						new Tuple2<Object, UserProperty>(7L, new UserProperty("grace", "mentor", 40))));

		JavaRDD<Edge<String>> myEdge = sc.parallelize(Arrays.asList(new Edge<String>(1L, 3L, "Collegue"),
				new Edge<String>(1L, 2L, "Collegue"), new Edge<String>(3L, 5L, "Collegue"),
				new Edge<String>(7L, 5L, "advisor"), new Edge<String>(2L, 3L, "Collegue")));
		// 输出顶点的信息
		myVertices.collect().forEach(f -> System.out
				.println(f._1.toString() + f._2.name + "###########" + f._2.title + "*********************"));
		myVertices.collect().forEach(f -> System.out.println(f));
		// 输出边的信息
		// myEdge.foreach(s->
		// System.out.println(s.attr+"#######################"));

		// 利用RDD的filter操作，构造满足条件的边集合
		JavaRDD<Edge<String>> myEdgeFilter = myEdge.filter(f -> f.attr.contains("a"));
		myEdgeFilter.collect().forEach(p -> System.out.println("check the filtered egdes" + p));

		org.apache.spark.graphx.Graph<UserProperty, String> myGraph = org.apache.spark.graphx.Graph.apply(
				myVertices.rdd(), myEdge.rdd(), null, StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), tagUser,
				tagString);

		// 数出图顶点当中属性是intern的点的个数
		Long num = myGraph.vertices().filter(new SerializableFunction1<Tuple2<Object, UserProperty>, Object>() {
  
			@Override
			public Boolean apply(Tuple2<Object, UserProperty> tuple) {
				// TODO Auto-generated method stub

				if (tuple._2.getTitle().equals("intern"))
					return true;
				else
					return false;

			}
		}).count();
		showTripletList(myGraph);

		System.out.println("*vertex with intern property" + num);

		// myGraph.vertices().filter(Function1<Tuple2<Object,UserProperty>,
		// Object>function1)
		System.out.println("***********************" + myGraph.vertices().first().toString());

		// 属性图的基本集合操作，这些操作采用用户自定义的函数，并产生包含转化特征和结构的新.
		// 定义在Graph类当中的是图的核心操作，它是经过优化的实现
		// 表示核心操作的组合的便捷操作定义在GraphOps中
		/**
		 * Graph的属性：numEdges, numVertices, VertexRDD[Int]
		 * inDegrees,VertexRDD[Int] outDegrees, VertexRDD[Int] degrees
		 */
		// 对图的顶点和边的属性替换操作失败，不知道给予的函数参数怎么表示
		// Graph<VertexProperty,String> newVertices =
		// myGraph.mapVertices(object,tagNewUser,
		// scala.Predef.$eq$colon$eq<UserProperty, VertexProperty> eq);

		/*
		 * subgraph函数操作， public abstract Graph<VD,ED>
		 * subgraph(scala.Function1<EdgeTriplet<VD,ED>,Object> epred,
		 * scala.Function2<Object,VD,Object> vpred) epred - the edge predicate,
		 * which takes a triplet and evaluates to *true* if the edge is to
		 * remain in the subgraph. Note that only edges where both vertices
		 * satisfy the vertex predicate are considered.
		 * 
		 * vpred - the vertex predicate, which takes a vertex object and
		 * evaluates to true if the vertex is to be included in the subgraph
		 */
		// subgraph 先利用vpre挑选出符合条件的顶点构成的图，再用epre限制边的条件挑选出边
		org.apache.spark.graphx.Graph<UserProperty, String> aSubGraph = myGraph
				.subgraph(new SerializableFunction1<EdgeTriplet<UserProperty, String>, Object>() {
					public Boolean apply(EdgeTriplet<UserProperty, String> tuple) {
						// if(tuple.attr.equals("advisor"))
						return true;
						// else return false;
					}
				}, new SerializableFunction2<Object, UserProperty, Object>() {
					// 搞不懂这里的第一个object有啥子用处
					public Boolean apply(Object e, UserProperty tuple) {

						if (tuple.getTitle().equals("intern"))
							return true;
						else
							return false;

					}
				});

		JavaRDD<Tuple2<Object, UserProperty>> subVertice = aSubGraph.vertices().toJavaRDD();
	//	aSubGraph.outerJoinVertices(arg0, arg1, arg2, arg3, arg4)
		System.out.println("check sub vertice" + subVertice.count() + "check sub edge" + aSubGraph.edges().count());
		showTripletList(aSubGraph);
        
		 GraphImpl<UserProperty, String> implGraph = GraphImpl.apply(myVertices.rdd(), myEdge.rdd(), null, StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), tagUser,
					tagString);
		 System.out.println("GraphImpl operation"+implGraph.edges().first().toString());
		
		// 将原图中有向边的顺序逆转，获得图的reverse
		org.apache.spark.graphx.Graph<UserProperty, String> reverseGraph = myGraph.reverse();
		showTripletList(reverseGraph);

		System.out.println("Show subgraph first " + aSubGraph.vertices().first().toString()
				+ aSubGraph.edges().first().toString());

		// 相邻聚合函数例子 计算follower比自己大的人的个数
		VertexRDD<msg> verts = myGraph.aggregateMessages(sendMsg, mergeMsg, TripletFields.All, tagMsg);
		System.out.println("count the vertices" + verts.count());

		VertexRDD<msg> newVerts = (VertexRDD<msg>) verts.mapValues(new SerializableFunction1<msg, msg>() {
			public msg apply(msg tuple) {
				return new msg(tuple.getNum(), tuple.getAge() / tuple.getNum());
			}
		}, tagMsg);
        verts.toJavaRDD();
		
        
        /**
         * InnerJoin Section 笔记：
         * Function里面的参数Api要求的是什么就给他什么，这里要将JavaRDD转换成RDD
         * Function里面的Object能给出当前处理的vertice的Id是多少
         * 
         */
		VertexRDD<VertexProperty> innerJoinVertice =  myGraph.vertices().innerJoin(JavaRDD.toRDD(verts.toJavaRDD()), new SerializableFunction3<Object,UserProperty ,msg, VertexProperty>() {

			@Override
			public VertexProperty apply(Object a, UserProperty b, msg c) {
				// TODO Auto-generated method stub
				System.out.println("index" + a.toString());
				VertexProperty result = new VertexProperty(b.getName(),c.getNum());
				return result;
				}
			}, tagMsg, tagJoinUser);
		  verts.toJavaRDD().foreach(f->System.out.println("*********Oringial msg verts"+ f));
		  innerJoinVertice.toJavaRDD().foreach( f->System.out.println("****************inner join vertice result" +f));
		
		  /**
		   * Left Join Section
		   */
//		  VertexRDD<VertexProperty> leftJoinVertice = myGraph.vertices().leftJoin(JavaRDD.toRDD(verts.toJavaRDD()),new SerializableFunction3<Object, UserProperty,scala.Option<Class<UserProperty>>,  VertexProperty>() {
//
//		
//
//			@Override
//			public VertexProperty apply(Object arg0, UserProperty arg1, Option<Class<UserProperty>> arg2) {
//				// TODO Auto-generated method stub
//				return null;
//			}
//
//		 
//			
//		}, tagUser, tagJoinUser);
//		 map(new SerializableFunction1<Tuple2<Object, msg>, msg>(){
//		 public msg apply(Tuple2<Object, msg> tuple){
//		 tuple.setAge(input.getAge()/input.getNum());
//		 return new msg(1, 2);
//		 }
//		 }, tagMsg);
		verts.toJavaRDD().foreach(f -> System.out.println("result vertexRDD" + f));
		newVerts.toJavaRDD().foreach(f -> System.out.println("result of average VertexRDD" + f));
		// GraphOps<UserProperty, String> opsOfGraph = new GraphOps<>(myGraph,
		// tagUser, tagString);
		myGraph.ops().degrees().toJavaRDD()
				.foreach(f -> System.out.println("Show the input degree of every vertice" + f));
		;
 
 
 /**
  * Pagerank Section
  */
		// Build a graph from a file
		SparkContext sparkContext = sc.toSparkContext(sc);
//		org.apache.spark.graphx.Graph<Object, Object> pageRankGraph = GraphLoader.edgeListFile(sparkContext,
//				"Cit-HepTh.txt", false, -1, StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY());
//		// Perform pageRank algorithm
//		Tuple2<Object, Object> result = pageRankGraph.ops().inDegrees().reduce(
//				new SerializableFunction2<Tuple2<Object, Object>, Tuple2<Object, Object>, Tuple2<Object, Object>>() {
//					public Tuple2<Object, Object> apply(Tuple2<Object, Object> a, Tuple2<Object, Object> b) {
//						Integer i = Integer.parseInt(a._2.toString());
//						Integer j = Integer.parseInt(b._2.toString());
//						return i > j ? a : b;
//					}
//				});
//					
//						
//		org.apache.spark.graphx.Graph<Object, Object> rankedGraph = pageRankGraph.ops().pageRank(0.001, 0.15);
//		System.out.println("Whatever result" + rankedGraph.vertices().count());
//		Tuple2<Object, Object> highestRank = rankedGraph.vertices().reduce(
//				new SerializableFunction2<Tuple2<Object, Object>, Tuple2<Object, Object>, Tuple2<Object, Object>>() {
//					public Tuple2<Object, Object> apply(Tuple2<Object, Object> a, Tuple2<Object, Object> b) {
//
//						return (Double) a._2 > (Double) b._2 ? a : b;
//					}
//				});
//		System.out.println("Highest rank" + highestRank);
//		System.out.println("Count the vertices of file build graph" + pageRankGraph.vertices().first().toString());
//         
		sc.stop();
	}
   
	

	static void showTripletList(org.apache.spark.graphx.Graph<UserProperty, String> inputGraph) {
		JavaRDD<String> tripletList = inputGraph.triplets()
				.map(new SerializableFunction1<EdgeTriplet<UserProperty, String>, String>() {
					@Override
					public String apply(EdgeTriplet<UserProperty, String> tuple) {
						// TODO Auto-generated method stub
						return tuple.srcAttr().getName() + " is the " + tuple.attr + " of " + tuple.dstAttr().getName();
					}
				}, tagString).toJavaRDD();

		tripletList.collect().forEach(f -> System.out.println("***************" + f));
	}

}

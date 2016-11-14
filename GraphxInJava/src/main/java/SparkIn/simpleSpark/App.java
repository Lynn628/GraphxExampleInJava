package SparkIn.simpleSpark;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.ipc.Server.Call;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.EdgeContext;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.TripletFields;
import org.apache.spark.graphx.VertexRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.storage.StorageLevel;

import scala.Function;
import scala.Function1;
import scala.Function2;
import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.BoxedUnit;
import org.apache.spark.graphx.lib.*;
/**
 * Hello world!
 *
 */


public class App 
{
	
	static abstract class SerializableFunction1<T1, R> 
	extends AbstractFunction1<T1, R> implements Serializable {}
	
	static abstract class SerializableFunction2<T1, T2, R>
	extends AbstractFunction2<T1, T2, R> implements Serializable {}
	
	
//	class GetLength implements Function<String, Integer>{
//		public Integer call(String s){
//			return s.length();
//		}
//	}
//	
//	class Sum implements Function2<Integer, Integer, Integer>{
//		public Integer call(Integer a, Integer b){
//			return a + b;
//		}
//	}
//	
//	   class Graph<VD, ED>{
//    	   VertexRDD<VD> vertices;
//    	   EdgeRDD<ED> edges;
//    	   
//          
//      } 
    public static void main( String[] args )
    {  
    	
    	JavaSparkContext sc = new JavaSparkContext( 
    			new SparkConf().setMaster("local").setAppName("EdgeCountJava"));
    	
        JavaRDD<Tuple2<Object, String>> myVertices = sc.parallelize(Arrays.asList(
        		new Tuple2<Object, String>(1L, "Ann"), new Tuple2<Object, String>(2L, "Bill"),
        		new Tuple2<Object, String>(3L, "Charles"), new Tuple2<Object, String>(4L, "Diane"),
        		new Tuple2<Object, String>(5L, "Went to the gym this morning")));
        
        JavaRDD<Edge<String>>myEdges = sc.parallelize(Arrays.asList(
        		new Edge<String>(1L, 2L, "is-friends-with"),
        		new Edge<String>(2L, 3l, "is-friends-with"),
        		new Edge<String>(3L, 4L, "is-friends-with"),
        		new Edge<String>(4L, 5L, "Likes-status"),
        		new Edge<String>(3L, 5L, "Wrote-status")));
        
        Graph<String, String>myGraph = Graph.apply(myVertices.rdd(),
        		myEdges.rdd(), "", StorageLevel.MEMORY_ONLY(),
        		StorageLevel.MEMORY_ONLY(), tagString, tagString);
        
    	Graph<Integer, String>initialGraph = myGraph.mapVertices(
    			new SerializableFunction2<Object, String, Integer>(){
    				public Integer apply(Object o, String s) {return 0;}
    			}, 
    			tagInteger, null);
    	
//    	List<Tuple2<Object, Integer>> ls = toJavaPairRDD(
//    			propagateEdgeCount(initialGraph).vertices(), tagInteger).collect();
//    	
//    	for(Tuple2<Object, Integer>t: ls)
//    	    System.out.print(t + "**");
//    	
//    	Graph<Integer,Long> pageRank = run(myGraph,)
//    	System.out.println();
    	sc.stop();
    	
//    	String logFile = "D:/JavaJars/spark-2.0.0-bin-hadoop2.7.tgz/spark-2.0.0-bin-hadoop2.7/README.md";
//        SparkConf conf = new SparkConf().setAppName("simpleSpark").setMaster("local");
//        JavaSparkContext sc = new JavaSparkContext(conf);
//        JavaRDD<String> logData = sc.textFile(logFile).cache();
//        
//        List <String> data = Arrays.asList("Acfun","Apple","Boolean","Bite","Karry");
//        JavaRDD<String> distData = sc.parallelize(data);
//        
////        JavaRDD<Integer> lineLengths = logData.map(s -> s.length());
////        int totalLength = lineLengths.reduce((a,b) -> a+b );
//        
//        long numAs = logData.filter(new Function<String, Boolean>(){
//        	public Boolean call(String s){ return s.contains("a");} 
//        }).count();
//        
//        long numAstr = distData.filter(new Function<String, Boolean>(){
//        	public Boolean call(String s){ return s.contains("a");} 
//        }).count();
//        
//        long numBs = logData.filter(new Function<String, Boolean>(){
//        	public Boolean call(String s){ return s.contains("b");}
//        }).count();
//        
//        long numBstr = distData.filter(new Function<String, Boolean>(){
//        	public Boolean call(String s){ return s.contains("B");}
//        }).count();
//        
//        System.out.println("Lines with a " + numAs + ", lines with b "+ numBs + "\n");
//        System.out.println("Lines with a " + numAstr + ", lines with b "+ numBstr + "\n");
////        JavaRDD<Integer> lineLengths = logData.map(new GetLength());
////        int totalLength = lineLengths.reduce(new Sum()); 
////        System.out.println("Lines amount of logData is " + totalLength);
//        JavaPairRDD<String, Integer> pairs = logData.mapToPair(s -> new Tuple2(s,1));
//        JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a+b);
//         		
//        		
//        		List<Edge> edgesList = new ArrayList();
//        		edgesList.add(new Edge(1L, 2L, "is-friends-with"));
//        		edgesList.add(new Edge(2L, 3L, "is-friends-with"));
//        		edgesList.add(new Edge(3L, 4L, "is-friends-with"));
//        		edgesList.add(new Edge(4L, 5L, "Likes-status"));
//        		edgesList.add(new Edge(3L, 5L, "Wrote-status"));
//        		
//        		//EdgeRDD<ED> myedges = new EdgeRDD(sc, edges);
//        	   
//                System.out.println("******************edge count");
//                  
//    
//               //Graph<VD, ED> graph = new EgdeRDD(sc,edgesList);
    	
    } 

    //Must explicitly provide for implicit Scala parameters in various function calls
    //classTag<T>保存了被泛型擦出后的原始类型T，提供给运行时的
    private static final ClassTag<Integer> tagInteger = ClassTag$.MODULE$.apply(Integer.class);
    private static final ClassTag<String> tagString = ClassTag$.MODULE$.apply(String.class);  
    private static final ClassTag<Object> tagObject = ClassTag$.MODULE$.apply(Object.class); 
   
    //sendMsg
    private static final SerializableFunction1<
    EdgeContext<Integer, String, Integer>, BoxedUnit> sendMsg = 
    new SerializableFunction1<
    	EdgeContext<Integer, String, Integer>, BoxedUnit>(){
    	public BoxedUnit apply(EdgeContext<Integer, String, Integer>ec)
    		{
    		ec.sendToDst(ec.srcAttr() + 1);
    		return BoxedUnit.UNIT;
    	}
    };
    
    //mergeMsg
    private static final SerializableFunction2<Integer, Integer, Integer> mergeMsg = 
    		new SerializableFunction2<Integer, Integer, Integer>(){
    	public Integer apply(Integer a, Integer b){
    		return Math.max(a, b);
    	}
    };
    
    
    private static <T> JavaPairRDD<Object, T>toJavaPairRDD(VertexRDD<T> v, ClassTag<T> tagT){
    	return new JavaPairRDD<Object, T>((RDD<Tuple2<Object, T>>) v, tagObject, tagT);
    	
    }
    
    
//    private static Graph<Integer, String> propagateEdgeCount(
//    		Graph<Integer, String> g) {
//    	VertexRDD<Integer> verts = g.aggregateMessages(sendMsg, mergeMsg, TripletFields.All, tagInteger);
//    	Graph<Integer, String> g2 = Graph.apply(verts, g.edges(), 0, StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), tagInteger, tagString);
//    	int check = toJavaPairRDD(g2.vertices(), tagInteger)
//    			    .join(toJavaPairRDD(g.vertices(), tagInteger))
//    			    .map(new Function1<Tuple2<Object, Tuple2<Integer, Integer>>, Integer>() {
//    		public Integer  call(Tuple2<Object, Tuple2<Integer, Integr>> t){
//    			return t._2._1 - t._2._2;
//    			
//    		}
//    	}).reduce(new Function2<Integer, Integer, Integer>(){
//    		public Integer call(Integer a, Integer b) {return a+b; }
//    	});
//    	
//    	if(check > 0)
//    			return propagateEdgeCount(g2);
//    		
//        else 
//    	    return g;
//    	}
    }
    
    


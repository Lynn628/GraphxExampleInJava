package LinkedData;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.EdgeContext;
import org.apache.spark.graphx.EdgeTriplet;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.VertexRDD;
import org.apache.spark.storage.StorageLevel;

import LinkedData.HITS.VertexProperty;
import scala.Function1;
import scala.Tuple10;
import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.AbstractFunction3;
import scala.runtime.BoxedUnit;

		public class HITS {
			static abstract class SerializableFunction1<T1, R> extends AbstractFunction1<T1, R> 
			implements Serializable {
		
		}
		
		static abstract class SerializableFunction2<T1, T2, R> extends AbstractFunction2<T1, T2, R>
			implements Serializable {
		
		}
		
		static abstract class SerializableFunction3<T1, T2, T3, R> extends AbstractFunction3<T1, T2, T3, R>
		    implements Serializable{
		
		
		
		}
		
		static class VertexProperty  implements Serializable{
		
		private Double hub;
		private Double authority;
		
		public VertexProperty(Double hub, Double authority) {
			this.hub = hub;
			this.authority = authority;
			// TODO Auto-generated constructor stub
		}
		public void setAuthority(Double authority) {
			this.authority = authority;
		}
		
		public Double getAuthority() {
			return authority;
		}
		
		public void setHub(Double hub) {
			this.hub = hub;
		}
		public Double getHub() {
			return hub;
		}
		
		@Override
			public String toString() {
				
				return "Hub is" + Double.toString(this.hub) +
						" " + "Authority is" + Double.toString(this.authority);
			}
		
		
		}
		

		private static final ClassTag<Integer> tagInteger = ClassTag$.MODULE$.apply(Integer.class);
		private static final ClassTag<String> tagString = ClassTag$.MODULE$.apply(String.class);
		private static final ClassTag<Double> tagDouble = ClassTag$.MODULE$.apply(Double.class);
	//	private static final ClassTag<UserProperty> tagUser = ClassTag$.MODULE$.apply(UserProperty.class);
		//private static final ClassTag<msg> tagMsg = ClassTag$.MODULE$.apply(msg.class);
	//	private static final ClassTag<UserProperty> tagNewUser = ClassTag$.MODULE$.apply(VertexProperty.class);
		private static final ClassTag<VertexProperty> tagVertex = ClassTag$.MODULE$.apply(VertexProperty.class);
		//public static final scala.Option<UserProperty> optionUser = scala.Option$.MODULE$.apply(new UserProperty());
		
		// private static final scala.Predef.$eq$colon$eq<UserProperty,
		// VertexProperty> preChange = scala.Predef$.MODULE$.;
		
		
		public static void main(String[] args) {
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setMaster("local").setAppName("EdgeCount"));
		//构造顶点集
		JavaRDD<Tuple2<Object, VertexProperty>>netVertices = 
				sc.parallelize(Arrays.asList(new Tuple2<Object, VertexProperty>(1L,new VertexProperty(0.0, 0.0)),
						new Tuple2<Object, VertexProperty>(2L,new VertexProperty(0.0, 0.0)),
						new Tuple2<Object, VertexProperty>(3L,new VertexProperty(0.0, 0.0)),
						new Tuple2<Object, VertexProperty>(4L,new VertexProperty(0.0, 0.0)),
						new Tuple2<Object, VertexProperty>(5L,new VertexProperty(0.0, 0.0)),
						new Tuple2<Object, VertexProperty>(6L,new VertexProperty(0.0, 0.0)),
						new Tuple2<Object, VertexProperty>(7L,new VertexProperty(0.0, 0.0))
						));
		
	   //构造边集
		JavaRDD<Edge<Double>> netEdges = sc.parallelize(Arrays.asList(new Edge<Double>(1L, 3L, 0.0),
				new Edge<Double>(3L, 6L, 0.0),
				new Edge<Double>(5L, 1L, 0.0),
				new Edge<Double>(5L, 2L, 0.0),
				new Edge<Double>(2L, 1L, 0.0),
				new Edge<Double>(4L, 7L, 0.0)));
	
		//构造Base图
		Graph<VertexProperty, Double> baseGraph = Graph.apply(netVertices.rdd(), 
				netEdges.rdd(), null, StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(), 
				tagVertex, tagDouble);
		//输出base图的triplets
		showTripletList(baseGraph);
		
		//进入HITS
		//1.将顶点的hub authority值置为1
	    baseGraph.triplets().foreach(new SerializableFunction1<EdgeTriplet<VertexProperty,Double>, BoxedUnit>() {

			@Override
			public BoxedUnit apply(EdgeTriplet<VertexProperty, Double> arg0) {
				arg0.srcAttr().setAuthority(1.0);
				arg0.dstAttr().setAuthority(1.0);
				arg0.srcAttr().setHub(1.0);
				arg0.dstAttr().setHub(1.0);
				System.out.println(arg0.dstAttr().getAuthority());
				// TODO Auto-generated method stub
				return null;
			}
		});
	    System.out.println("********************Initial through triplets");
	    System.out.println( baseGraph.triplets().first().srcAttr().authority);
	    showTripletList(baseGraph);
	    System.out.println("*******************Check vertex");
	    baseGraph.vertices().toJavaRDD().foreach(f -> System.out.println(f._2.authority));
	    
		baseGraph.vertices().toJavaRDD().foreach(f->{f._2.setAuthority(1.0); f._2.setHub(1.0);});
		baseGraph.vertices().foreach(new SerializableFunction1<Tuple2<Object,VertexProperty>, BoxedUnit>() {
			@Override
			public BoxedUnit apply(Tuple2<Object, VertexProperty> vertice) {
				vertice._2.setHub(1.0);
				vertice._2.setAuthority(1.0);
				//System.out.println(vertice._2.getHub());
				return null;
			} 
		});
//		JavaRDD<Tuple2<Object,VertexProperty>> initializedVeritces = baseGraph.vertices().toJavaRDD();
//		System.out.println("**************intialized vertice"); 
//		initializedVeritces.foreach(f->System.out.println(f._2.toString()));
	   
//		System.out.println("*****************after initiated the veritce" );
//		System.out.println("******************show changed veritce"+ baseGraph.vertices().first()._2.getAuthority());
//		baseGraph.vertices().toJavaRDD().foreach(f-> System.out.println(f._2.getAuthority()));
//		System.out.println("Show triplets src authority");
//		baseGraph.triplets().toJavaRDD().foreach(f->System.out.println(f.srcAttr().getAuthority()));
//		showTripletList(baseGraph);
		
		//2.
		}
		
		//输出图
		static void showTripletList(Graph<VertexProperty,Double> inputGraph) {
			JavaRDD<String> tripletList = inputGraph.triplets()
					.map(new SerializableFunction1<EdgeTriplet<VertexProperty, Double>, String>() {
						@Override
						public String apply(EdgeTriplet<VertexProperty, Double> tuple) {
							// TODO Auto-generated method stub
							return tuple.srcAttr().getAuthority() + " is the " + tuple.attr + " of " + tuple.dstAttr().getAuthority();
						}
					}, tagString).toJavaRDD();

			tripletList.collect().forEach(f -> System.out.println("***************" + f));
		}

		
		//进入HITS算法
		
		}
package SparkIn.simpleSpark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.EdgeDirection;
import org.apache.spark.graphx.EdgeTriplet;
import org.apache.spark.graphx.Graph;
import org.apache.spark.graphx.Pregel;
import org.apache.spark.graphx.util.GraphGenerators;
import org.jets3t.service.utils.RestUtils;

import com.twitter.chill.config.ScalaAnyRefMapConfig;

import scala.Equals;
import scala.Option;
import scala.Predef;
import scala.Predef.$eq$colon$eq;
import scala.Tuple2;
import scala.annotation.elidable;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;
import scala.reflect.internal.Trees.New;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.AbstractFunction3;
import scala.tools.nsc.transform.patmat.Logic.PropositionalLogic.Eq;



	
	public class SingleSourceShortestPath {
	
		static abstract class SerializableFunction1<T1, R> extends AbstractFunction1<T1, R>
		implements Serializable{
			
		}
		
		static abstract class SerializableFunction2<T1, T2, R> extends AbstractFunction2<T1, T2, R>
		implements Serializable{
			
		}
		
		static abstract class SerializableFunction3<T1, T2, T3, R> extends AbstractFunction3<T1, T2, T3, R>
		implements Serializable{
			
		}
		
		private static final ClassTag<Double> tagDouble = ClassTag$.MODULE$.apply(Double.class);
	
		private static final $eq$colon$eq<Object,Double> eqDouble = new $eq$colon$eq<Object, Double>(){
		   public Double apply(Object arg0) {
			     return Double.parseDouble(arg0.toString());
		   };
		};
		
		
		public static void main(String[] args) {
		
		SparkContext sc = new JavaSparkContext(new SparkConf().setMaster("local").setAppName("EdgeCount")).sc();
		Graph<Object, Double>shortGraph = GraphGenerators.logNormalGraph(sc, 100, 1, 4.0, 1.3, -1).mapEdges(new SerializableFunction1<Edge<Object>, Double>(){
	
			@Override
			public Double apply(Edge<Object> a) {
				
				return Double.parseDouble(a.attr().toString());
			}
		
		},tagDouble);
		
		Integer verteId = 42;
		Graph<Double, Double> initalGraph = shortGraph.mapVertices(new SerializableFunction2<Object, Object, Double>() {
			@Override
			public Double apply(Object a, Object b) {
				if(Double.parseDouble(a.toString()) == verteId)
				b = 0.0;
				else 
				b = Double.POSITIVE_INFINITY;
				return (Double)b;
			}
			
		}, tagDouble,eqDouble);
		
		 Pregel.apply(initalGraph, Double.POSITIVE_INFINITY, Integer.MAX_VALUE, EdgeDirection.Out(),
		  //  scala.Function3<Object,VD,A,VD> vprog 
		  new SerializableFunction3<Object, Double, Double,Double>() {

			@Override
			public Double apply(Object a, Double b, Double c) {
			    System.out.println("b before"+ b);
				b = Math.min(b, c);
				System.out.println("b after" + b);
				return b;
			}
		},
		  // scala.Function1<EdgeTriplet<VD,ED>,scala.collection.Iterator<scala.Tuple2<Object,A>>> sendMsg
		  new SerializableFunction1<EdgeTriplet<Double,Double> ,scala.collection.Iterator<scala.Tuple2<Object, Double>>>(){

			/**
			 *  public Iterator<Tuple2<Object, Integer>> apply(EdgeTriplet<Integer, Boolean> et) {
        System.out.println(et.srcId()+" ---> "+et.dstId()+" with: "+et.srcAttr()+" ---> "+et.dstId());

        if (et.srcAttr() > et.dstAttr()) {
            return JavaConverters.asScalaIteratorConverter(Arrays.asList(et.toTuple()._1()).iterator()).asScala();
        }else{
            return JavaConverters.asScalaIteratorConverter(new ArrayList<Tuple2<Object, Integer>>().iterator()).asScala();
        }
    }
			 */
			@Override
			public Iterator<Tuple2<Object, Double>> apply(EdgeTriplet<Double, Double> a) {
				// TODO Auto-generated method stub
				System.out.println("SrcId"+a.srcId()+"SrcAttr"+a.srcAttr()+"EdgeAttr"+a.attr()+"dstId"+a.dstId()+"dstAttr"+a.dstAttr() );
				if(a.srcAttr() + a.attr() < a.dstAttr()){
				return  JavaConverters.asScalaIteratorConverter(Arrays.asList(new Tuple2<Object, Double>(a.srcId(),a.srcAttr() + a.attr())).iterator()).asScala();
				}
				return JavaConverters.asScalaIteratorConverter(new ArrayList<Tuple2<Object, Double>>().iterator()).asScala();
			  }	
			}, 
		  // scala.Function2<A,A,A> mergeMsg,
		  new SerializableFunction2<Double, Double, Double>() {
			@Override
			public Double apply(Double a, Double b) {
				
				return Math.min(a, b);
			}
		}, tagDouble, tagDouble,tagDouble);
				//.vertices().toJavaRDD().foreach(f->System.out.println("Show the generated graph"+f));
	      sc.stop();
		}
}
	
	

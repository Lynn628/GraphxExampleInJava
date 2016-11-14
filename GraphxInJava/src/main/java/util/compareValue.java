package util;

import org.apache.commons.httpclient.methods.multipart.StringPart;

public class compareValue {
   void main(String[] args){
	   boolean result = compare(1.6180339631667064, 1.6180339985218035);
	  
   }
   
   Boolean compare(Double a, Double b){
	  if(a-b == 0){
		  return true;
	  }else
		  return false;
   }
}

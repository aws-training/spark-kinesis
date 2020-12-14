package model


import java.util.Date
import java.text.SimpleDateFormat


case class WordCount (val value : String, val count: BigInt){
	def toTuple  = {
		(value, count)
	}
}

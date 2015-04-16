package gsn.data.format

import gsn.data._
import scala.xml.XML
import scala.xml.Attribute
import scala.xml.UnprefixedAttribute
import scala.xml.Text
import scala.xml.Null
import scala.xml.Elem

object XmlSerializer extends DataSerializer{
  
  override def ser(data:Seq[SensorData],props:Seq[String],withVals:Boolean)={
    val xml=
      <gsn>{
      data.map(d=>toXml(d))
      }        
      </gsn>;
    xml.toString
  }
    
  override def ser(data:SensorData,props:Seq[String],withVals:Boolean)=
    toXml(data).toString
  
  private def toXml(data:SensorData):xml.Elem={
    val values=data.ts.map(_.series)
    val s= data.sensor 
    val desc=s.properties.getOrElse("description","")
    val acces=s.properties.getOrElse("accessProtected","false").toBoolean
    val protect=if (acces) "(protected)" else "" 
    var i = 1  
    val vs= 
      <virtual-sensor name={s.name} 
        protected={protect} 
        description={desc}>
        {
          val output = s.fields.map{f=> 
            var value = ""
            if (values.size >= i+1) {
              if (values(i).size >=1) {
                value = values(i)(0).toString()
              }
            }
            i+=1
            var tmp = <field name={f.fieldName} type={f.dataType.name} unit={f.unit.code}>{value}</field>
            addMappings(tmp, f.mapping)
          }
          
          val predicates=s.properties.map{case (k,v)=>
            <field name={k} catergory="predicate" >{v}</field>
          }
          output++predicates
        }
      </virtual-sensor>;
    vs
  }  
  private def addMappings(e:Elem,mappings:Option[List[(String, String)]]):Elem = {
    var acc = e
    mappings match {
      case Some(m) => m.foreach(x => acc = acc % Attribute(None, x._1, Text(x._2), Null))
      case None =>
    }
    acc
  }
}
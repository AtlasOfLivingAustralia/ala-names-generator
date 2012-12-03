package au.org.ala.names

import reflect.BeanProperty




class Term (@BeanProperty val id:Int, @BeanProperty val nomen:String, @BeanProperty val canonical:String, @BeanProperty rawVariants:Array[String]){
    val variants = rawVariants.map(v => v.toLowerCase.trim) 
}

/** Factory for terms */
object Term {
    def apply(id:Int,canonical: String): Term = new Term(id,"",canonical, Array[String]())
    def apply(id:Int,canonical: String, variant: String): Term = new Term(id,"", canonical, Array(variant))
    def apply(id:Int,canonical: String, variants: String*): Term = new Term(id,"", canonical, Array(variants:_*))
    def apply(id:Int, canonical: String, variants: Array[String]): Term = new Term(id, "", canonical, variants)
}

trait Vocab {
  

  val all:Set[Term]

  val regexNorm = """[ \\"\\'\\.\\,\\-\\?]*"""
  
  def getTermForId(id:Int) : Option[Term] ={
      all.foreach(term =>{
          if(term.getId == id)
              return Some(term)
      })
      None
  }
      
  def matchTerm(string2Match:String, nomen:String) : Option[Term] = {
    if(string2Match!=null){
      //strip whitespace & strip quotes and fullstops & uppercase
      val stringToUse = string2Match.replaceAll(regexNorm, "").toLowerCase
      
      
      //println("string to use: " + stringToUse)
      all.foreach(term => {          
        //println("matching to term " + term.canonical)
        if(term.canonical.equalsIgnoreCase(stringToUse) &&(term.nomen.equalsIgnoreCase(nomen) || term.nomen.equals("")))
          return Some(term)
        if(term.variants.contains(stringToUse) &&(term.nomen.equalsIgnoreCase(nomen) || term.nomen.equals(""))){
          return Some(term)
        }
      })
    }
    None
  }
  
   def loadVocabFromFile(filePath:String) : Set[Term] = {
    scala.io.Source.fromURL(getClass.getResource(filePath), "utf-8").getLines.toList.map({ row =>
        val values = row.split("\t")
        val id = Integer.parseInt(values(0))
        val nomen = values(1)        
        val variants = values.tail.tail.map(x => x.replaceAll("""[ \\"\\'\\.\\,\\-\\?]*""","").toLowerCase)        
        new Term(id, nomen,values(2), variants)
    }).toSet
   }
  
}

object Ranks extends Vocab {
  val all = loadVocabFromFile("/ranks.txt")
  def main(args: Array[String]): Unit = {
    val nomen = "Zoological"
      println(Ranks.matchTerm("subf", nomen).get.getCanonical);
      println(Ranks.matchTerm("subvar",nomen).get.getCanonical)
      println(Ranks.matchTerm("Tribe", nomen).get.getCanonical)
      println(Ranks.matchTerm("Division", nomen).get.getCanonical)
      println(Ranks.matchTerm("Cohort", nomen).get.getCanonical)
      println(Ranks.matchTerm("Higher Taxon", nomen).get.getCanonical)
      println(Ranks.matchTerm("Aggregate Species", nomen).get.getCanonical)
      println(Ranks.matchTerm("Aggregate Genera",nomen).get.getCanonical)
      println(Ranks.matchTerm("Division", nomen).get.getCanonical)
      println(Ranks.matchTerm("Division", "Botanical").get.getCanonical)
  }
}

object NomenTerms {
    val Botanical = List("Plantae", "Fungi", "Chromista", "Protozoa")
    val Zoological = List("Animalia")
}
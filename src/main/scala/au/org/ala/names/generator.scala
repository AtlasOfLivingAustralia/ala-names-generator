package au.org.ala.names

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue
import au.org.biodiversity.services.taxamatch.impl.TaxamatchServiceImpl
import scala.collection.mutable.ArrayBuffer
import au.org.ala.util.OptionParser

object NamesGenerator {
  val alaDAO = new AlaConceptsJDBCDAO
  val asynDAO = new AlaSynonymJDBCDAO
  val mergeDAO = new MergeAlaConceptsJBBCDAO
  val tcDAO = new TaxonConceptJDBCDAO
  val tnDAO = new TaxonNameJDBCDAO
  val relDAO = new RelationshipJDBCDAO
  val classDAO = new AlaClassificationJDBCDAO
  val mergeclassDAO = new MergeAlaClassificationJDBCDAO
  val colTcDAO = new ColConceptsJDBCDAO
  val extraDAO = new ExtraNamesJDBCDAO
  val colDAO = new ColConceptsJDBCDAO
  val padDAO = new NamesListPaddingJDBCDAO
  val nlnDAO = new NamesListNameJDBCDAO
  val nlDAO = new NamesListJDBCDAO
  val db = new BoneCPDatabase
  //indicates that the init threads should stop
  var stopInit = false
  //used to store genera
  val namecache = new org.apache.commons.collections.map.LRUMap(10000)
      val blacklist = scala.io.Source.fromURL(getClass.getResource("/blacklist.txt"), "utf-8").getLines.toList.map({value =>
      val values = value.split("\t")
      values(0)})
  private val lock : AnyRef = new Object()
  def main(args: Array[String]) {
    var all = false
    var most = false
    var lftRgtNSL = false
    var initNames =false
    var addApni=false
    var padCaab=false
    var padCol=false
    var createClass=false
    var shouldAddMissingKingdoms = false
    var cleanOldDumps = false
    var colLftRgt = false
    var tnSoundEx=false
    var mergeTc=false
    var mergeClass=false
    var padUsingLists=false
    var padType=""
    var synSoundEx = false
    var dwcFile:Option[String] = None
    val parser = new OptionParser("ALA Name Generation Options") {
            opt("nsl","Preprocesses the NSL records so to determine classification depths",{lftRgtNSL=true})
            opt("init","Initialise the NSL taxon names to be used in ALA",{initNames = true})
            opt("apni","Initialise the APNI taxon names to be used in ALA",{addApni = true})
            opt("caab","Pads out AFD data with missing CAAB species",{padCaab=true})
            opt("lists","Pads out the ala concepts with data in the names_list table based on configured ",{s:String =>padUsingLists=true;padType=s})
            opt("col","Pads out AFD data with missing CoL species that have at least one occurrence in Australia",{padCol = true})
            opt("kingdoms","Adds missing CoL kingdoms ot the ALA names",{shouldAddMissingKingdoms = true})
            opt("class","Generate the final classification",{createClass = true})
            opt("all","Perform all phases in the correct order",{all = true})
            opt("most","Performs all the phases EXCEPT nsl",{most=true})
            opt("clean", "Rename the old dumps so that the mysql dumps will not fail", {cleanOldDumps=true})
            opt("collft","Generates left rigth values for the Col table",{colLftRgt = true})
            opt("tnexp","Generates the sounds like expressions for the taxon names",{tnSoundEx=true})
            opt("synexp","Generates the sounds like expressions for the ala-synonyms",{synSoundEx=true})
            opt("merge","Merge the remaining COL concepts into the ALA concepts - allowing original to remain separate" , {mergeTc=true})
            opt("mergecl","Create the merged classification" , {mergeClass=true})
            opt("dwc","a dwc file to load in the names list tables - used to pad out NSL", {s:String => dwcFile=Some(s)})
        }
        println("Starting " + new java.util.Date)
        if(parser.parse(args)){
            
          db withSession {
            if(dwcFile.isDefined){
              DwCANamesLoader.loadArchive(dwcFile.get)
            }
            if(mergeClass)
              generateClassification(true, mergeclassDAO)
            if(mergeTc)
              generateMergedConcepts()
            if(all || tnSoundEx)
              generateTaxonNamesSoundExName()
            if(colLftRgt)
              generateLftRgtForCoL()
            if(all || lftRgtNSL)
              generateLftRgtDepthForNSL()
            if(all || most || initNames){
              initialiseAlaConcept(true)
              fillMissingParents()
            }
            if(all || most || synSoundEx){
              generateSynonymSoundEx()
            }
            if(all || most || addApni){
              stopInit = false
              initialiseApniConcepts()
            }
            if(all || most || padCaab){
              padAFDCaab()
            }
            if(all || most|| padUsingLists){
              stopInit = false
              if(!(all || most)){
                padUsingNamesLists(padType)
              } else{
                padUsingNamesLists("all")
                stopInit = false
                padUsingNamesLists("merge")
              }
            }
            if(all || most || padCol){
              padAFDCol()
            }
            if(shouldAddMissingKingdoms){
              //NQ: This is no longer included as default because we have generic method for performing this in names lists
              addMissingColKingdoms()
            }
            if(all || most || createClass)
              generateClassification(true, classDAO)
            if(all || most || cleanOldDumps)
              prepareForDump()
          }
        }
        else
          parser.showUsage
        println("Ending " + new java.util.Date)
  }
  def generateSynonymSoundEx(){
    println("Generating the sound ex for the NSL synonyms")
    val queue = new ArrayBlockingQueue[AlaSynonymDTO](500)
    val arrayBuf = new ArrayBuffer[Thread]()
    var i = 0
    while (i < 10) {
      val t = new Thread(new GenericQueueThread[AlaSynonymDTO](queue, i,{syn => {
        //get the taxon name
        //println(syn)
        val tn =tnDAO.getByLsid(syn.nameLsid.getOrElse(""))
        if(tn.isDefined){
          val(genex, spex, inspex) = generateSoundExs(tn.get)
          if(genex.isDefined){
            //we need to update the sounds like expressions
            asynDAO.updateSoundEx(syn.lsid, genex, spex,inspex)
          }
        }
      } 
      })
      )
      t.start
      arrayBuf += t
      i += 1
    }
    
    asynDAO.iterateOverTN({item=>{
      while (queue.size >= 499) {
        Thread.sleep(50)
      }
      queue.add(item)
      true
    }})
    
    stopInit = true
    arrayBuf.foreach(t => t.join)
  }
  /**
   * Pads the NSL with the lists that have been loaded 
   */
  def padUsingNamesLists(padType:String){
    //get all the names list merge items
    val list = padDAO.getListsByPadType(padType)
    //create the array thread queue stuff
    val queue = new ArrayBlockingQueue[NamesListPaddingDTO](30)
    val arrayBuf = new ArrayBuffer[Thread]()
    var i = 0
    while (i < 10) {
      val t = new Thread(new GenericQueueThread[NamesListPaddingDTO](queue, i,{pad => {
        performPadList(pad)
//        pad.padType match{
//          case "all" => performPadList(pad)
//          case "merge" =>//performPadList(pad)
//          case _ => println("Unsupported pad type " + pad.padType)
//        }
      }}))
      t.start
      arrayBuf += t
      i += 1
    }
    list.foreach(item =>{
      //
      while (queue.size >= 29) {
        Thread.sleep(50)
      }      
      queue.add(item)
    })
    stopInit = true
    arrayBuf.foreach(t => t.join)
    //now add all the synonyms for the processed lists items
    nlnDAO.addSynonymsForPadType(padType)
    nlnDAO.updateExcludedStatus()
  }
  /**
   * Adds all the supplied concepts without checking for clashes.
   */
  def performPadList(nlp:NamesListPaddingDTO){
    println(new java.util.Date() + " Starting to pad for " + nlp)
    //get the list details
    val nl = nlDAO.getNamesListById(nlp.id).get
    val src = Some(nl.name)
    //if scientific name exists 
    if(nlp.scientificName.isDefined){
      //get the name as it appears in the names list
      val rootNameListItem = nlnDAO.getByNameAndList(nlp.id, nlp.scientificName.get)
      println(new java.util.Date() + " List root item " + rootNameListItem)
      //obtain the NSL concept for the supplied name
      val nslRootItem = tnDAO.getTaxonNameIfIncluded(nlp.scientificName.get, rootNameListItem.nomenCode.getOrElse("Zoological"))
      //if the nslRootItem is empty need to check if one of the other lists provided a concept for this
      val acceptedListItem = nlnDAO.getNameListItemInALA(nlp.scientificName.get)
      println(new java.util.Date() + " NSL root item " + nslRootItem)
      val parentLsid = if(nslRootItem.isDefined)alaDAO.getAlaConceptForName(nslRootItem.get.lsid).get.lsid else if (acceptedListItem.isDefined) acceptedListItem.get.lsid else rootNameListItem.lsid
      println(new java.util.Date() + " Parent LSID: "+ parentLsid)
      //add the root object if necessary
      if(parentLsid == rootNameListItem.lsid){
        //add it
        /*
         * case class AlaConceptsDTO(id: Option[Int], lsid: String, nameLsid: Option[String], parentLsid: Option[String],
  parentSrc: Option[Int], src: Option[Int], acceptedLsid: Option[String], rankId: Option[Int], synonymType: Option[Int], genusSoundEx: Option[String], speciesSoundEx: Option[String], infraSoundEx: Option[String],
  source: Option[String])
         */
        //lookup the rank
        val rank = Ranks.matchTerm(rootNameListItem.rank.getOrElse(""), rootNameListItem.nomenCode.getOrElse("Zoological"));
        //default rankId is unranked
        val rankId = if (!rank.isEmpty) Some(rank.get.getId) else Some(9999)
        
        val alac = new AlaConceptsDTO(None, parentLsid, None, None,None,None,None,rankId,None,rootNameListItem.genex, rootNameListItem.spex, rootNameListItem.inspex, src)
        alaDAO.insertNewTerm(alac)
        
      }
      //retrieve all the children for he names list root item      
      recursivelyAddChildrenAll(nlp.id,rootNameListItem.lsid, parentLsid, src,if(nlp.padType=="all"){name =>processListAll(name)} else{name => processListMerge(name)})
    } else{
      //we need to add all the entries that are floating at the supplied level that are NOT synonyms
      val floating = nlnDAO.getByListAndEmpty(nlp.id, nlp.taxonRank.getOrElse(""))
      floating.foreach{item =>
        //add the item
        //lookup the rank
        val rank = Ranks.matchTerm(item.rank.getOrElse(""), item.nomenCode.getOrElse("Zoological"));
        //default rankId is unranked
        val rankId = if (!rank.isEmpty) Some(rank.get.getId) else Some(9999)
        val alac = new AlaConceptsDTO(None, item.lsid, None, None,None,None,None,rankId,None,item.genex, item.spex, item.inspex, src)
        alaDAO.insertNewTerm(alac)
        recursivelyAddChildrenAll(nlp.id, item.lsid, item.lsid,src,{name => processListEmpty(name)})
      }
    }
  }
  def processListEmpty(name:NamesListNameDTO):(Boolean,String) ={
    def alaConcept = alaDAO.getConceptByLsid(name.lsid)
    def add = (name.acceptedLsid.isEmpty || name.acceptedLsid.get == name.lsid) && alaConcept.isEmpty
    (add, null)
  }
  /**
   * Decides on whether or not an "all" names list name should be included.
   */
  def processListAll(name:NamesListNameDTO):(Boolean,String) ={
    def add = name.acceptedLsid.isEmpty || name.acceptedLsid.get == name.lsid
    (add, null)
  }
  /**
   * Decides whether or not a "merge" names list name should be included. This is based on whether or not
   * it already exists as an ala-concept.  "Merge" names should be processed after the "all" names.
   */
  def processListMerge(name:NamesListNameDTO):(Boolean, String)={
    if(name.specificEpithet.isEmpty){
        //for genus level or above attempt to locate an exact match already in the list
        val nom = if(name.kingdom.getOrElse("") == "Animalia") "Zoological" else "Botanical"
        //val tn = tnDAO.getTaxonNameIfIncluded(name.scientificName,nom)
        val ala = alaDAO.getConceptBasedOnNameAndCode(name.scientificName, nom)
        if(ala.isEmpty){
          val nslsyn = asynDAO.getSynonymWithNameNomen(name.scientificName, nom)
          if(nslsyn.isEmpty){
            //check to see if it has been added as part of a previous list merge
            val nln = nlnDAO.getNameListItemInALA(name.scientificName)
            if(nln.isEmpty){
              (true, null)
            } else{
              (false, nln.get.lsid)
            }
          } else{
            (false, nslsyn.get.lsid)
          }
        } else{
          (false, ala.get.lsid)
        }
    } else{
      //species or infraspecies attempt to locate a match based on the sound expressions.
      //look for accepted
      val alsid = alaDAO.getMatchingSoundEx(name.genex.getOrElse(""), name.spex.getOrElse(""), name.inspex)
      if(alsid.length>0){
        (false, alsid.head)
      } else{
          //look for synonym
        val slsid = asynDAO.getMatchingSoundEx(name.genex.getOrElse(""), name.spex.getOrElse(""), name.inspex)
        if(slsid.length > 0){
          (false, slsid.head)
        } else{
          (true, null)
        }
      }
    }
    
  } 
  val blacklisted = List("Incertae sedis","Not assigned")
  def recursivelyAddChildrenAll(listId:Int,listParentLsid:String, alaParentLsid:String, src:Option[String], checkToAdd:(NamesListNameDTO => (Boolean,String))){
    //get the list of children for the supplied parent
    nlnDAO.getByParentAndList(listId, listParentLsid).foreach{child =>
        var newParent = child.lsid
        //add the child if it is not a blacklisted  name
        if(!blacklisted.contains(child.scientificName)){
          val rank = Ranks.matchTerm(child.rank.getOrElse(""), child.nomenCode.getOrElse("Zoological"));
          //default rankId is unranked
          val rankId = if (!rank.isEmpty) Some(rank.get.getId) else Some(9999)
          val (shouldAdd, alaConceptLsid) = checkToAdd(child)
          if(shouldAdd){
            val alaConcept = new AlaConceptsDTO(None, child.lsid, None, Some(alaParentLsid),None,None,None,rankId, None, child.genex, child.spex, child.inspex, src)
            alaDAO.insertNewTerm(alaConcept)            
          } else if(alaConceptLsid != null){
            newParent = alaConceptLsid
          }
        } else{
          newParent = alaParentLsid
        }
        //add its children
        recursivelyAddChildrenAll(listId,child.lsid, newParent, src, checkToAdd)
        
    }
  }
//  def performPadMerge(nlp:NamesListPaddingDTO){
//    println(new java.util.Date() + " Starting to pad merge for " + nlp)
//    //get the list details
//    val nl = nlDAO.getNamesListById(nlp.id).get
//    val src = Some(nl.name)
//    //if scientific name exists 
//    if(nlp.scientificName.isDefined){
//      
//    }
//    
//  }
  
  //TODO FIX the COL padding this is not looking at CAAB before adding a species...
  def padAFDCol(){
          
   
    val genusMap = new scala.collection.mutable.HashMap[String, Option[String]]
    var famMap = new scala.collection.mutable.HashMap[String, Option[String]]
    var orderMap = new scala.collection.mutable.HashMap[String, Option[String]]
    var classMap = new scala.collection.mutable.HashMap[String, Option[String]]
    scala.io.Source.fromURL(getClass.getResource("/animals_col_biocache.txt")).getLines.foreach{ line =>
      val values = line.split(",");

      //get the col concept
      val colConcept = colDAO.getSimilarConcept(values(1),"Animalia")
      if(colConcept.isDefined){
        val gse = getSoundEx(colConcept.get.genusName.get, false)
        val sse = getSoundEx(colConcept.get.speciesName.get, true)
        val source = Some("CoL")
        val list = if(gse.isDefined && sse.isDefined) alaDAO.getMatchingSoundEx(gse.get, sse.get, None) else List()

        if(list.size>0 || alaDAO.isNameSynonyms(values(1)) || alaDAO.isNameAccepeted(values(1))){
          //do nothing
        } else{
          //now we need to add a CoL concept
          //println("Need to add " + line)
          var parent = genusMap.get(colConcept.get.genusName.get)
          if(parent.isEmpty){
            val ac =alaDAO.getConceptBasedOnNameAndCode(colConcept.get.genusName.get, "Zoological")
            if(ac.isEmpty){
              //println("Missing genus : " +colConcept.get.genusName.get)
              
              var famParent = famMap.get(colConcept.get.familyName.get)
              if(famParent.isEmpty){
                val fam = alaDAO.getConceptBasedOnNameAndCode(colConcept.get.familyName.get, "Zoological")
                if(fam.isEmpty){
                  //println("Missing family : " + colConcept.get.familyName.get)
                  //get the order
                  var orderParent = orderMap.get(colConcept.get.orderName.get)
                  if(orderParent.isEmpty){
                    val orderConcept = alaDAO.getConceptBasedOnNameAndCode(colConcept.get.orderName.get, "Zoological")
                    if(orderConcept.isEmpty){
                      //get the class if we have already located it
                      var classParent = classMap.get(colConcept.get.className.get)
                      if(classParent.isEmpty){
                        //attempt to locate it in AFD
                        var classConcept =  alaDAO.getConceptBasedOnNameAndCode(colConcept.get.className.get, "Zoological")
                        if(classConcept.isEmpty){
                          parent = Some(None)
                          classMap.put(colConcept.get.className.get,None)
                          println("Missing Class: " +colConcept.get.className.get)
                        }
                        else{
                          //it needs to be added to the map
                          classMap.put(colConcept.get.className.get, Some(classConcept.get.lsid))
                          classParent = Some(Some(classConcept.get.lsid))
                        } 
                      }
                      //create the order 
                      val orderColConcept = colDAO.getConcept(colConcept.get.orderId.get)
                      if(orderColConcept.isDefined){
                        orderParent = Some(Some(orderColConcept.get.lsid))
                        orderMap.put(colConcept.get.orderName.get, orderParent.get)
                        //now insert the order
                        val alaConcept = new AlaConceptsDTO(None, orderColConcept.get.lsid, Some(orderColConcept.get.lsid), if(classParent.isDefined)classParent.get else None, Some(400), Some(400), None, Some(4000), None, None, None, None, source)
                        alaDAO.insertNewTerm(alaConcept)
                      }
                    }
                    else{
                      orderMap.put(colConcept.get.orderName.get, Some(orderConcept.get.lsid))
                      orderParent = Some(Some(orderConcept.get.lsid))
                    }
                      //println("Missing Class: " + colConcept.get.className.get)
                  }
                  //add the missing family
                  val famCon = colDAO.getConcept(colConcept.get.familyId.get)
                  if(famCon.isDefined){
                    // add col fmail
                    famParent=Some(Some(famCon.get.lsid));
                    famMap.put(famCon.get.familyName.get, Some(famCon.get.lsid))
                    val alaConcept = new AlaConceptsDTO(None, famCon.get.lsid, Some(famCon.get.lsid), if(orderParent.isDefined)orderParent.get else None, Some(400), Some(400), None, Some(5000), None, None, None, None, source)
                    alaDAO.insertNewTerm(alaConcept)
                  }
                  else
                    famParent =Some(None)
                }
                else{
                  famParent = Some(Some(fam.get.lsid))
                  famMap.put(colConcept.get.familyName.get,Some(fam.get.lsid))
                }
              }
              //add the missing genus
              //get the col genus by id
              val genCon = colDAO.getConcept(colConcept.get.genusId.get)
              if(genCon.isDefined){
                //add the col genus
                genusMap.put(genCon.get.genusName.get,Some(genCon.get.lsid))
                val alaConcept = new AlaConceptsDTO(None, genCon.get.lsid, Some(genCon.get.lsid), famParent.get, Some(400), Some(400), None, Some(6000), None, gse, None, None, source)
                alaDAO.insertNewTerm(alaConcept)
                parent = Some(Some(genCon.get.lsid))
                //println(genusMap)
              }
            }
            else{
              parent = Some(Some(ac.get.lsid))
              genusMap.put(colConcept.get.genusName.get, parent.get)
            }
          }
          //NOW add the missing Col species
          val alaConcept = new AlaConceptsDTO(None, colConcept.get.lsid, Some(colConcept.get.lsid), parent.get, Some(400), Some(400), None, Some(7000), None, gse, sse, None, source)      
          alaDAO.insertNewTerm(alaConcept)
        }
      }
    }
  }
  
  /**
   * Inserts the CAAB species that are missing from AFD
   */
  def padAFDCaab(){
    extraDAO.truncate()    
    var process = false
    val genusMap = new scala.collection.mutable.HashMap[String, Option[String]]
    var famMap = new scala.collection.mutable.HashMap[String, Option[String]]
    //According to Tony Rees' email dated 2013-01-24 these are the correct mappings for the AFD missing families 
    famMap.put("Tetrabrachiidae", Some("urn:lsid:biodiversity.org.au:afd.taxon:bbcc4ea1-7641-47de-9f61-8d8810149216"))
    famMap.put("Pteroidae", Some("urn:lsid:biodiversity.org.au:afd.taxon:a4c50e6d-3d8b-4020-89d4-09018af2cb22"))
    famMap.put("Odacidae", Some("urn:lsid:biodiversity.org.au:afd.taxon:8c04dbc2-fb26-4128-9e3a-ae416357097c"))
    scala.io.Source.fromURL(getClass.getResource("/caab_names.csv")).getLines.foreach{ line =>
      if(process){
        val values = line.split("\t")
        val gse = getSoundEx(values(5), false)
        val sse = getSoundEx(values(6), true)
        val list = if(gse.isDefined && sse.isDefined) alaDAO.getMatchSoundExSource(gse.get, sse.get, None,"AFD") else List()

        if(list.size>0 || alaDAO.isNameSynonyms(values(1)) || alaDAO.isNameAccepeted(values(1))){
          //do nothing
        } else{
          //println("NOT FOUND: " + line)
          //need to add the missing CAAB species
          val source = Some("CAAB")
          //get the parent for the CAAB concept
          var parent = genusMap.get(values(5))
          if(parent.isEmpty){
            val ac =alaDAO.getConceptBasedOnNameAndCode(values(5), "Zoological")
            if(ac.isEmpty){
              //println("Unable to locate " + values(5))
              genusMap.put(values(5),Some("CAAB_"+values(5)))
              extraDAO.insertNewTerm(new ExtraNamesDTO("CAAB_"+values(5), values(5), "", values(3), values(4),values(5),""))
              var famParent = famMap.get(values(4))
              if(famParent.isEmpty){
                val fam = alaDAO.getConceptBasedOnNameAndCode(values(4), "Zoological")
                if(fam.isDefined){
                  famParent = Some(Some(fam.get.lsid))
                  famMap.put(values(4),Some(fam.get.lsid))
                }
                else
                  println("Unable to locate " + values(4))
              }
              //add the missing genus
              val alaConcept = new AlaConceptsDTO(None, "CAAB_"+values(5), Some("CAAB_"+values(5)), famParent.get, Some(200), Some(200), None, Some(6000), None, gse, sse, None, source)
              alaDAO.insertNewTerm(alaConcept)              
              parent = Some(Some("CAAB_"+values(5)))
            }
            else{
              parent = Some(Some(ac.get.lsid))
              genusMap.put(values(5), Some(ac.get.lsid));
            }
          }
          //add the missing species
          val alaConcept = new AlaConceptsDTO(None, values(0), Some(values(0)), parent.get, Some(200), Some(200), None, Some(7000), None, gse, sse, None, source)      
          alaDAO.insertNewTerm(alaConcept)
          extraDAO.insertNewTerm(new ExtraNamesDTO(values(0), values(1), values(2), values(3), values(4),values(5),values(6)))
        }
      } else
        process =true
    }
    println(genusMap)
    println(famMap)
  }
  
  def handleBlacklistedNames(){
    //As of 2013-01-15: the only blacklisted names are ones that have a Unplaced or Unknown suffix and ranked above species level
    val acs = alaDAO.getUnknownUnplacedConcepts();
    acs.foreach{ac =>{
      //check to see if the parent is "Unplaced"
      var parentLsid = ac.parentLsid
      if(ac.parentLsid.isDefined){
        val parent = tcDAO.getByLsid(ac.parentLsid.get)
        val sciName = parent.scientificName.toLowerCase()
        if(sciName.startsWith("unknown") || sciName.startsWith("undefined"))
          parentLsid = parent.parentLsid
      }
      //now update the children of these to point to a non-blacklisted parent
      alaDAO.updateChidrenParentRefs(Some(ac.lsid), parentLsid)
      //now remove the blacklisted name
      alaDAO.deleteConcept(ac.lsid)
    }}
  }
  
  def prepareForDump(){
    //rename the existing dump files
    //org.apache.commons.io.FileUtils.
    val conFileName ="/data/bie-staging/ala-names/ala_accepted_concepts_dump"
    val synFileName="/data/bie-staging/ala-names/ala_synonyms_dump"
    val idFileName="/data/bie-staging/ala-names/identifiers"
    val afdCommonFile = "/data/bie-staging/ala-names/AFD-common-names"
    val alaHomonymsFile = "/data/bie-staging/ala-names/ala-species-homonyms"
    val date = new java.util.Date().getTime()
    renameFile(conFileName+".txt",conFileName+date+".txt")
    renameFile(synFileName+".txt", synFileName+date+".txt")
    renameFile(idFileName+".txt", idFileName+date+".txt")
    renameFile(afdCommonFile+".csv", afdCommonFile+date+".csv")
    renameFile(alaHomonymsFile+".txt", alaHomonymsFile+date+".txt")
  }
  
  def renameFile(source:String, target:String){
    try{
    org.apache.commons.io.FileUtils.moveFile(new java.io.File(source), new java.io.File(target))
    }
    catch{
      case e:Exception => e.printStackTrace()
    }
  }
  /**
   * Generates the sounds like expression for the names
   */
  def generateTaxonNamesSoundExName(){
    
    println("Starting to generate the sound expressions for the TaxonName table")      
    var count = 0
    val start = System.currentTimeMillis
    var startTime = System.currentTimeMillis
    var finishTime = System.currentTimeMillis

    val queue = new ArrayBlockingQueue[TaxonNameDTO](500)
    val arrayBuf = new ArrayBuffer[Thread]()
    var i = 0
    while (i < 25) {
      val t = new Thread(new AlaInitThread(queue, i,{tn => {
        val(genEx, spEx, inSpEx) = NamesGenerator.generateSoundExs(tn)
        //minimally need a sounds ex for the genus before we update
        if(genEx.isDefined)
          tnDAO.updateSoundsLikeExpressions(tn.lsid, genEx, spEx, inSpEx)
      }}))
      t.start
      arrayBuf += t
      i += 1
    }

    tnDAO.iterateOver(tn => {

      while (queue.size >= 490) {
        Thread.sleep(50)
      }
      if(!blacklist.contains(tn.lsid))
        queue.add(tn)
      count += 1
      if (count % 1000 == 0) {
        finishTime = System.currentTimeMillis
        println(count
          + " >> Last key : " + tn.lsid
          + ", records per sec: " + 1000f / (((finishTime - startTime).toFloat) / 1000f)
          + ", time taken for " + 1000 + " records: " + (finishTime - startTime).toFloat / 1000f
          + ", total time: " + (finishTime - start).toFloat / 60000f + " minutes")
        startTime = System.currentTimeMillis
      }

      true
    })

    stopInit = true
    arrayBuf.foreach(t => t.join)
          
  }
  
  def generateMergedConcepts(){    
    println("Starting to generate merged ala concepts with CoL")
    //Step one copy across all the concepts from the ala_concept table
    //Step two extract all the CoL concepts, species level or lower, that are NOT already in the table
    //Step three only add if it is not part of the same nomenclatural group with a sounds like expression that matches.
    
    var count = 0
    val start = System.currentTimeMillis
    var startTime = System.currentTimeMillis
    var finishTime = System.currentTimeMillis

    val queue = new ArrayBlockingQueue[ColConceptsDTO](500)
    val arrayBuf = new ArrayBuffer[Thread]()
    var i = 0
    while (i < 25) {
      val t = new Thread(new CoLMergeThread(queue, i))
      t.start
      arrayBuf += t
      i += 1
    }
    
    colDAO.getAnimalPlantFamiles().foreach{fam =>
      while (queue.size >= 490) {
        Thread.sleep(50)
      }
      queue.add(fam)
      count += 1
      if (count % 1000 == 0) {
        finishTime = System.currentTimeMillis
        println(count
          + " >> Last key : " + fam.lsid
          + ", records per sec: " + 1000f / (((finishTime - startTime).toFloat) / 1000f)
          + ", time taken for " + 1000 + " records: " + (finishTime - startTime).toFloat / 1000f
          + ", total time: " + (finishTime - start).toFloat / 60000f + " minutes")
        startTime = System.currentTimeMillis
      }
    }
    stopInit=true
    arrayBuf.foreach(t => t.join)
    mergeDAO.deleteDuplicates()
  }
  
  def getNotNotAssignedParent(fam:ColConceptsDTO, nom:String):Option[String]={
    var validParent=0 
    if(fam.orderName.get != "Not assigned"){
      validParent = fam.orderId.get
    }
    else if(fam.className.get != "Not assigned"){
      validParent = fam.classId.get 
    }
    else if(fam.phylumName.get != "Not assigned")
      validParent = fam.phylumId.get
    else
      validParent = fam.kingdomId.get
      
    //retrive the COL concept
    val col = colDAO.getConcept(validParent)
    if(col.isDefined){
      CachedObjects.getLock("family").synchronized{
        getColOrAlaConcept(col.get, nom)
      }
    }
    else
      None
  }
  def getColOrAlaConcept(col:ColConceptsDTO, nom:String):Option[String] ={
    var id = CachedObjects.getLsidFromCache(col.rank, col.scientificName)
    if(id.isEmpty){
      //attempt to find the ala
      val ala = alaDAO.getConceptBasedOnNameAndCode(col.scientificName, nom)
      if(ala.isDefined){
        CachedObjects.putLsidInCache(col.rank, col.scientificName, ala.get.lsid)
        id = Some(ala.get.lsid)
      }
      else{
        //need to add as col
        val parent = colDAO.getConcept(col.parentId.get)        
        val parentId = getColOrAlaConcept(parent.get, nom)
        val rank = Ranks.matchTerm(col.rank, nom) 
        val rankId = if (!rank.isEmpty) Some(rank.get.getId) else Some(9999)
        val alaConcept = new AlaConceptsDTO(None, col.lsid, Some(col.lsid), parentId , Some(405), Some(405), None, rankId, None, None, None, None, Some("CoL"))
        mergeDAO.insertNewTerm(alaConcept)
        CachedObjects.putLsidInCache(col.rank,col.scientificName, parentId.get)
        id = Some(col.lsid)
      }
    }
    id
  }
  
  def addMergedColFamilyConcepts(fam:ColConceptsDTO){
    val start = System.currentTimeMillis
    //STEP 1: Ensure that the higher level classification exists    
    val source = Some("CoL")
    //check to see if the family exist in ALA
    val nom = if(fam.kingdomName.get == "Animalia") "Zoological" else "Botanical"
    var famId = if(fam.scientificName == "Not assigned") getNotNotAssignedParent(fam, nom) else CachedObjects.getLsidFromCache("family", fam.scientificName)
    if(famId.isEmpty){
      //check to see if it is in the ALA
      val alafam = alaDAO.getConceptBasedOnNameAndCode(fam.scientificName, nom)
      if(alafam.isDefined){
        CachedObjects.putLsidInCache("family", fam.scientificName, alafam.get.lsid)
        famId = Some(alafam.get.lsid)
      }
      else{
        //need to add the family to the 
        CachedObjects.getLock("family").synchronized{
          //now insert a new family
          var ordId = CachedObjects.getLsidFromCache("order", fam.orderName.get)
          if(ordId.isEmpty){            
            val alaord = alaDAO.getConceptBasedOnNameAndCode(fam.orderName.get, nom)
            if(alaord.isDefined){
              CachedObjects.putLsidInCache("order", fam.orderName.get, alaord.get.lsid)
              ordId = Some(alaord.get.lsid)
            }
            else{
              //get the CoL order
              val orderColConcept = colDAO.getConcept(fam.orderId.get)
              if(orderColConcept.isDefined){
                ordId = Some(orderColConcept.get.lsid)
                //need to add an order
                var classId = CachedObjects.getLsidFromCache("class", fam.className.get)
                if(classId.isEmpty){
                  //check to see if it is in ala
                  val alaclass = alaDAO.getConceptBasedOnNameAndCode(fam.className.get, nom)
                  if(alaclass.isDefined){
                    CachedObjects.putLsidInCache("class", fam.className.get, alaclass.get.lsid)
                    classId = Some(alaclass.get.lsid)
                  }
                  else{
                    //needd to add a class
                    //get the CoL class
                    val classColConcept = colDAO.getConcept(fam.classId.get)
                    if(classColConcept.isDefined){
                      classId = Some(classColConcept.get.lsid)
                      var phylumId = CachedObjects.getLsidFromCache("phylum", fam.phylumName.get)
                      if(phylumId.isEmpty){
                        //check to see if it is the ala
                        val alaphylum = alaDAO.getConceptBasedOnNameAndCode(fam.phylumName.get, nom)
                        if(alaphylum.isDefined){
                          CachedObjects.putLsidInCache("phylum", fam.phylumName.get, alaphylum.get.lsid)
                          phylumId = Some(alaphylum.get.lsid)
                        }
                        else{
                          //need to add a new phylum
                          //get the CoL phylum
                          val phylumColConcept = colDAO.getConcept(fam.phylumId.get)
                          if(phylumColConcept.isDefined){
                            phylumId= Some(phylumColConcept.get.lsid)
                            var kingdomId = CachedObjects.getLsidFromCache("kingdom", fam.kingdomName.get)
                            if(kingdomId.isEmpty){
                              val alakingdom = alaDAO.getConceptBasedOnNameAndCode(fam.kingdomName.get, nom)
                              if(alakingdom.isDefined){
                                CachedObjects.putLsidInCache("kingdom", fam.kingdomName.get, alakingdom.get.lsid)
                                kingdomId = Some(alakingdom.get.lsid)
                              }
                            }
                            //parent in kingdomId
                            val alaConcept = new AlaConceptsDTO(None, phylumId.get, phylumId, kingdomId , Some(405), Some(405), None, Some(2000), None, None, None, None, source)
                            mergeDAO.insertNewTerm(alaConcept)
                            CachedObjects.putLsidInCache("phylum",fam.phylumName.get, phylumId.get)
                          }
                        }
                      }
                      //parent in phylumId
                      val alaConcept = new AlaConceptsDTO(None, classId.get, classId, phylumId , Some(405), Some(405), None, Some(3000), None, None, None, None, source)
                      mergeDAO.insertNewTerm(alaConcept)
                      CachedObjects.putLsidInCache("class",fam.className.get, classId.get)
                    }
                  }
                }
                 //parent in classId
                val alaConcept = new AlaConceptsDTO(None, ordId.get, ordId, classId , Some(405), Some(405), None, Some(4000), None, None, None, None, source)
                mergeDAO.insertNewTerm(alaConcept)
                CachedObjects.putLsidInCache("order",fam.orderName.get, ordId.get)
              }
            }
          }
          //parent in ordId
          val alaConcept = new AlaConceptsDTO(None, fam.lsid, Some(fam.lsid), ordId , Some(405), Some(405), None, Some(5000), None, None, None, None, source)
          mergeDAO.insertNewTerm(alaConcept)
          CachedObjects.putLsidInCache("family",fam.scientificName, fam.lsid)
          famId = Some(fam.lsid)
        }
      }
    }
    
    
    //STEP 2: Handle all the children that belong to the supplied family
    var count=0
    val species = colDAO.getSpeciesForFamily(fam.taxonId)
    //println(species)
    //store the current genus, species etc and then only need to update parent when they change
    var currentGenus:String=null
    var currentColGenus:Int=0
    var currentSpeciesLsid:String=null
    var currentSpeciesId=0
    species.foreach{ sp=>
      if(sp.genusId.isDefined){
        if(sp.genusId.get != currentColGenus){
          currentColGenus = sp.genusId.get
          //see if the current genus exists in ALA
          val alagenus = alaDAO.getConceptBasedOnNameAndCode(sp.genusName.get, nom)
          if(alagenus.isDefined)
            currentGenus = alagenus.get.lsid
            else{
              //add the col genus with the famId as a parent
              val colGenus = colDAO.getConcept(currentColGenus)
              currentGenus = colGenus.get.lsid              
              val alaConcept = new AlaConceptsDTO(None,colGenus.get.lsid, Some(colGenus.get.lsid), famId , Some(405), Some(405), None, Some(6000), None, None, None, None, source)
              mergeDAO.insertNewTerm(alaConcept)
            }
        }
        //now handle the species
        if(sp.rank == "species"){
          //check to see if my sounds like ex is handled in NSL
          val (genex, spex) =(getSoundEx(sp.genusName.get.trim, false), getSoundEx(sp.speciesName.get.trim, true))
          val list = if(genex.isDefined && spex.isDefined) tnDAO.getMatchSoundExNomen(genex.get,spex.get,None,nom) else List()
          if(list.size == 0){
            //we need to add the species
            currentSpeciesLsid = sp.lsid
            currentSpeciesId = sp.taxonId
            val alaConcept = new AlaConceptsDTO(None,currentSpeciesLsid, Some(currentSpeciesLsid), Some(currentGenus) , Some(405), Some(405), None, Some(7000), None, None, None, None, source)
            try{
              mergeDAO.insertNewTerm(alaConcept)
              count+=1
            }
            catch{
              case e:Exception => println("EXCEPTION: " + e.getMessage())
            }
            
          }
        }
        else{
          //ensure that the previous species id is the same as my id
          if(sp.speciesId.get == currentSpeciesId){
            //check to see if my sounds like ex is already handled
            val (genex, spex,inspex) =(getSoundEx(sp.genusName.get.trim, false), getSoundEx(sp.speciesName.get.trim, true), getSoundEx(sp.infraspeciesName.get.trim, true))
            val list = if(genex.isDefined && spex.isDefined && inspex.isDefined) tnDAO.getMatchSoundExNomen(genex.get,spex.get,inspex,nom) else List()
            if(list.size == 0){
              //we need to add the infraspecific concept
              val rank = Ranks.matchTerm(sp.rank, nom) 
              val rankId = if (!rank.isEmpty) Some(rank.get.getId) else Some(9999)
              val alaConcept = new AlaConceptsDTO(None,sp.lsid, Some(sp.lsid), Some(currentSpeciesLsid) , Some(405), Some(405), None, rankId, None, None, None, None, source)
              try{
                mergeDAO.insertNewTerm(alaConcept)
                count += 1
              }
              catch{
                case e:Exception => println("EXCEPTION: " + e.getMessage())
              }
            }
          }
        }
      }
    }
    val finish = System.currentTimeMillis()
    println("Finished processing " + fam.scientificName +" (" +famId+") " + " added " + count + " species in " + ((finish -start)/1000) + " seconds")
  }
  
  

  
  
  def generateLftRgtForCoL(){
    val start = System.currentTimeMillis
    var startTime = System.currentTimeMillis
    println("Starting to genrate lft and rgt values for CoL")
    val roots = colDAO.getMissingParentIds
    var finishTime = System.currentTimeMillis
    println("total time to get roots: " + (finishTime - start).toFloat / 60000f + " minutes")
    
    var left = 0
    var right = left
    roots.foreach { id =>
      right = handleLsid(id.asInstanceOf[Object], right, right + 1,{lsid =>colDAO.getChildrenIds(lsid.asInstanceOf[Int]).asInstanceOf[List[Object]]},{(lsid,currentDepth, lft, rgt)=>{
          //println("Updating " + lsid + " " + lft + " " + rgt)  
          colDAO.updateLftRgt(lsid, currentDepth, lft,rgt)}
      })
      finishTime = System.currentTimeMillis
      println(right
          + " >> Last key : " + id
          + ", records per sec: " + 1000f / (((finishTime - startTime).toFloat) / 1000f)
          + ", time taken for " + 1000 + " records: " + (finishTime - startTime).toFloat / 1000f
          + ", total time: " + (finishTime - start).toFloat / 60000f + " minutes")
        startTime = System.currentTimeMillis
    }
    finishTime = System.currentTimeMillis
    println("total time: " + (finishTime - start).toFloat / 60000f + " minutes")
  }

  /**
   * Genertaes the left right and depth values for the concepts in the taxon_concept table. This is
   * used to help determine the concepts that need to be included
   */
  def generateLftRgtDepthForNSL() {
    val start = System.currentTimeMillis
    println("Starting to load lft rgts etc ")
    val roots = tcDAO.getMissingParentIds

    var left = 0
    var right = left
    roots.foreach { id =>
      right = handleLsid(id, 1, right + 1,{lsid =>tcDAO.getChildrenIds(lsid.asInstanceOf[String])},{(lsid,currentDepth, left, right)=>tcDAO.update(lsid.asInstanceOf[String], currentDepth, left,right)})

    }
    var finishTime = System.currentTimeMillis
    println("total time: " + (finishTime - start).toFloat / 60000f + " minutes")
  }
  /**
   * A recursive function that will allow lft rgt value to be determined for the raw taxon_concept table
   */
  def handleLsid(lsid: Object, currentDepth: Int, currentLeft: Int, childProc: ((Object)=>List[Object]), updateProc:((Object,Int,Int,Int)=>Unit)): Int = {

    //get all the tc that have this one as a parent
    val children = childProc(lsid)//tcDAO.getChildrenIds(lsid)
    var left = currentLeft
    var right = left
    children.foreach { child =>
      right = handleLsid(child, currentDepth + 1, right + 1,childProc,updateProc)
      
    }

    //update the depth of the concept
    //println(lsid + " : depth : " + currentDepth + " left " + currentLeft + " right " + right+1)
    //tcDAO.update(lsid, currentDepth, currentLeft, right + 1)
    updateProc(lsid, currentDepth, currentLeft, right + 1)
    right + 1
  }
  /**
   * Insert all the APNI concepts.  An APNI concept is one of species level or below AND name is NOT convered by
   * APC.
   *
   * An APNI concept will be stand alone without synonyms.
   *
   */
  def initialiseApniConcepts() {
    var count = 0
    val start = System.currentTimeMillis
    var startTime = System.currentTimeMillis
    var finishTime = System.currentTimeMillis
    val queue = new ArrayBlockingQueue[String](500)
    val arrayBuf = new ArrayBuffer[Thread]()
    var i = 0
    while (i <30) {
      val t = new Thread(new ApniInitThread(queue, i))
      t.start
      arrayBuf += t
      i += 1
    }
    val list = alaDAO.getNamesLsidForApniConcepts()
    println("Checking " + list.size + " missing names ")
    list.foreach(lsid => {
      while (queue.size >= 490) {
        Thread.sleep(50)
      }
      if(!blacklist.contains(lsid))
        queue.add(lsid)
      count += 1
      if (count % 1000 == 0) {
        finishTime = System.currentTimeMillis
        println(count
          + " >> Last key : " + lsid
          + ", records per sec: " + 1000f / (((finishTime - startTime).toFloat) / 1000f)
          + ", time taken for " + 1000 + " records: " + (finishTime - startTime).toFloat / 1000f
          + ", total time: " + (finishTime - start).toFloat / 60000f + " minutes")
        startTime = System.currentTimeMillis
      }

      true
    })

    stopInit = true
    while (queue.size > 0)
      Thread.sleep(100)
    arrayBuf.foreach(_.join)

  }

  //initialises the ala concepts based on the taxon name groups
  /**
   * Initialises the ala concepts base on taxon name groups.  "Accepted" concepts will be identified and handled.
   * When an accepted concept is identified all of its synonyms are included.
   */
  def initialiseAlaConcept(truncate: Boolean) {
    if (truncate)
      alaDAO.truncate
    var count = 0
    val start = System.currentTimeMillis
    var startTime = System.currentTimeMillis
    var finishTime = System.currentTimeMillis

    val queue = new ArrayBlockingQueue[TaxonNameDTO](500)
    val arrayBuf = new ArrayBuffer[Thread]()
    var i = 0
    while (i < 25) {
      val t = new Thread(new AlaInitThread(queue, i,{tn => addConcept(tn, tn.lsid)}))
      t.start
      arrayBuf += t
      i += 1
    }

    tnDAO.iterateOver(tn => {

      while (queue.size >= 490) {
        Thread.sleep(50)
      }
      if(!blacklist.contains(tn.lsid))
        queue.add(tn)
      count += 1
      if (count % 1000 == 0) {
        finishTime = System.currentTimeMillis
        println(count
          + " >> Last key : " + tn.lsid
          + ", records per sec: " + 1000f / (((finishTime - startTime).toFloat) / 1000f)
          + ", time taken for " + 1000 + " records: " + (finishTime - startTime).toFloat / 1000f
          + ", total time: " + (finishTime - start).toFloat / 60000f + " minutes")
        startTime = System.currentTimeMillis
      }

      true
    })

    stopInit = true
    arrayBuf.foreach(t => t.join)
    println("Updating the broken parent references...")
    updateParentReferences
    println("Handling blacklisted...")
    handleBlacklistedNames()
    alaDAO.addExcludedConcepts()
    //inserts all the synonyms that are referenced by name instead of taxon concept.
    //also inserts the excluded relationships
    alaDAO.insertNameSynonyms()

  }
  /**
   * Updates the missing references so that all concepts reference parents that exist.
   */
  def updateParentReferences {
    alaDAO.updateMissingReferences
  }
  //run after initAlaConcepts.
  /**
   * Inserts missing parent concepts.  This usually comes about because there was a missing taxon name from the file. OR
   * the child is an "accepted" concept but the parent is not.
   */
  def fillMissingParents() {
    //fills the missing parent references. This happens when there are missing taxon names and/or concepts not correctly marked as accepted
    val list = alaDAO.getListOfMissingParents()
    println("Dealing with " + list.size + " missing concepts...")
    list.foreach(lsid => {
      addMissingConcept(lsid)
    })
  }
  /**
   * A recursive method that will add all missing ancestors to the ala concepts
   */
  def addMissingConcept(lsid: String) {
    val tc = tcDAO.getByLsid(lsid)
    println("Adding " + tc.lsid + " " + tc.rank)
    val tn = tnDAO.getByLsid(tc.nameLsid)
    val src = 300

    val parentSrc = if (tc.parentLsid.isEmpty) None else Some(50)
    val rankString = {
      if (tc.rank.length > 0)
        tc.rank
      else {
        //check to see if the name for the TC has a rank string
        val tn = tnDAO.getByLsid(tc.nameLsid)
        if (tn.isDefined)
          tn.get.rank
        else
          ""
      }
    }
    val rank = Ranks.matchTerm(rankString, "")
    //default rankId is unranked
    val rankId = if (!rank.isEmpty) Some(rank.get.getId) else Some(9999)
    val source = if(lsid.contains(":afd")) Some("AFD") else if(lsid.contains(":apni")) Some("APC") else None
    //
    //val (accepted, synonymType) = getAcceptedDetails(tc, list.size ==1)
    //generate the soundex for the taxon name
    val (gse, sse, ise) = if (tn.isDefined && (rankId.isEmpty || rankId.get > 6000)) generateSoundExs(tn.get) else (None, None, None)

    //alaDAO.addConcept(tc.lsid, Some(tc.nameLsid), tc.parentLsid, parentSrc, Some(src), accepted, rankId, synonymType,gse,sse,ise)

    //println(rankId)
    var alaConcept = new AlaConceptsDTO(None, tc.lsid, Some(tc.nameLsid), tc.parentLsid, parentSrc, Some(src), None, rankId, None, gse, sse, ise, source)
    //println(tc.lsid +" " + tc.nameLsid+ " " + tc.parentLsid + " " + parentSrc + " " + src + " " + accepted + " " + rankId + " " + synonymType+ " " + gse + " "+ sse +  " " + ise)
    //Some(alaConcept)

    //now check for missing parents
    if (tc.parentLsid.isDefined) {
      val parent = tcDAO.getByLsid(tc.parentLsid.get)
      if (parent != null) {
        val parConcept = alaDAO.getAlaConceptForName(parent.nameLsid)
        if (parConcept.isDefined) {
          //check to see if the return concept is the correct parent..
          if (parConcept.get.lsid != parent.lsid)
            alaConcept = new AlaConceptsDTO(None, tc.lsid, Some(tc.nameLsid), Some(parConcept.get.lsid), parentSrc, Some(src), None, rankId, None, gse, sse, ise, source)
        } else {
          addMissingConcept(tc.parentLsid.get)
        }
      }
    }

    alaDAO.insertNewTerm(alaConcept)

    //insert all the synonyms associated with this concept
    val synList = relDAO.getAllSynonymRelationships(tc.lsid)
    synList.foreach(it => {
      alaDAO.insertSynonym(tc.lsid, it._1, it._2, it._3)
    })
  }

  /**
   * Inserts the COL concepts.  We are only interested in kingdoms that are not covered in the NSL
   */
  def addMissingColKingdoms() {
    val start = System.currentTimeMillis
    println("Adding the missing CoL concepts")
    alaDAO.addMissingConceptsFromCoL()
    //now add their synonyms
    alaDAO.addColSynonyms()
    val end = System.currentTimeMillis()
    println("Finished adding missing CoL " + ((end - start) / 1000) + " seconds ")
  }
  /**
   * adds a single APNI concept to the ala concepts only if there is no similar name already there.
   */
  def addApniConcept(nameLsid: String) :Option[String]= {
    
    val list = tcDAO.getConceptsForName(nameLsid)
    if (list.size > 0) {
      val tc = list.head
      val taxonName = tnDAO.getByLsid(nameLsid)
      if (taxonName.isDefined) {
        val (gse, sse, ise) = generateSoundExs(taxonName.get)
        //now check to see if there is an existing APC concept with the same sound ex
        val list = if(gse.isDefined && sse.isDefined) alaDAO.getMatchSoundExSource(gse.get, sse.get, ise,"APC") else List()
       
        if (list.size > 0) {
          println("Unable to add " + nameLsid + " " + taxonName.get.scientificName + " soudexs: " + list)
        } else {
          //add the apni concept
          //get the rank id
          val rankString = {
            if (tc.rank.length > 0)
              tc.rank
            else {
              //check to see if the name for the TC has a rank string                      
              if (taxonName.isDefined)
                taxonName.get.rank
              else
                ""
            }
          }
          
          val rank = Ranks.matchTerm(rankString, taxonName.get.nomenCode)
          val rankId = if (!rank.isEmpty) Some(rank.get.getId) else Some(9999)
          val parent = findApniParent(tc, taxonName.get, rankId.get)
          val alaConcept = new AlaConceptsDTO(None, tc.lsid, Some(tc.nameLsid), parent, None, None, None, rankId, None, gse, sse, ise, Some("APNI"))
          lock.synchronized{
            val currentAlaConcept = alaDAO.getAlaConceptForName(nameLsid) 
            if(currentAlaConcept.isDefined)
              return Some(currentAlaConcept.get.lsid)
            else{
               //check to see if the same name already exists as a different lsid - only check if it a family level or above
              val sameConceptDiffName:Option[AlaConceptsDTO] = if(rankId.get >= 6000) None else {
                    val other =tnDAO.getTaxonName(taxonName.get.scientificName, None ,"Botanical").collectFirst{case result if result.lsid != taxonName.get.lsid => result}            
                    val otherAlaConcept = if(other.isDefined) alaDAO.getAlaConceptForName(other.get.lsid) else None
                    otherAlaConcept
              }
              if(sameConceptDiffName.isDefined){
                println("ALREADY ALA CONCEPT for same name different name lsid: " + sameConceptDiffName.get)
                return Some(sameConceptDiffName.get.lsid)
              }
              else
                alaDAO.insertNewTerm(alaConcept)
            }
          }
          return Some(tc.lsid)
          //alaDAO.addAlaConcept(tc.lsid, nameLsid, rankId.get,gse,sse,ise,"APNI");
        }
      }
    }
    return None
  }
  
  def findApniParent(tc:TaxonConceptDTO, tn:TaxonNameDTO, rankId:Int):Option[String]={
    var parent = if(tc.parentLsid.isDefined) alaDAO.getNameBasedParent(tc.parentLsid.get ) else None
    if(parent.isEmpty){
      //generate the parent based on the genera ie attempt to locate an APC genera that is the same as the APNI
      if(rankId >=7000 && rankId!=9999){
        val potentialParent = getGenusName(tn)
        if(potentialParent.isDefined)
          parent = Some(potentialParent.get.lsid)
      }
      //add the missing parent concept to the list of APNI concepts if it is not a synonym.
      if(parent.isEmpty && tc.parentLsid.isDefined){
        if(!alaDAO.isSynonym(tc.parentLsid.get)){
          val assignedParent = tcDAO.getByLsid(tc.parentLsid.get)
          parent =addApniConcept(assignedParent.nameLsid)
        }
      }
    }
    parent
  }
  
      def getGenusName(tn:TaxonNameDTO):Option[AlaConceptsDTO]={
        val nomenCode = tn.nomenCode match{
                case "Zoological" => "Zoological"
                case _ => "Botanical"
            }
        
        var value:Option[AlaConceptsDTO] = lock.synchronized{namecache.get((tn.genus, nomenCode)).asInstanceOf[Option[AlaConceptsDTO]]}
        if(value == null){
        lock.synchronized{    
        val genusMatch = tnDAO.getTaxonName(tn.genus,None, nomenCode)
                    if(genusMatch.size == 1){
                        val tn = genusMatch.head
                        println("Genus matched: " + tn.lsid + " : "+ tn.scientificName )
                        //TODO check Genus level homonym in IRMNG
                        
                        //If not a homonym find the corresponding alaConcepts for this name
                        value =alaDAO.getAlaConceptForName(tn.lsid)
                        
                        //value = Some(tn)
                    }
                    else
                        value = None
                    namecache.put((tn.genus,nomenCode), value)
        }
        }
        value
    }
  
  
  /**
   * Adds a concepts to the ala concepts. 
   */
  def addConcept(tn: TaxonNameDTO, taxonName: String): Option[AlaConceptsDTO] = {
    val list = tcDAO.getAcceptedConceptsForName(taxonName)
    if (!list.isEmpty) {

      //var src= -1
      //var parentSrc= -1
      var multiAccepeted = false
      var lastAccepted = false
      var acceptedCount = 0
      var tc = list.head
      //add the term
      //The SRC types
      //10 - Only accepted concept w/o synonym
      //20 - First accepted concept w/o synonym
      //30 - Choosing the concept that has the largest depth and most children
      val src = {
        val hasMultipleAccepted = list.size > 1 && list(1).isAccepted.equals("Y")
        tc match {
          case tc if tc.isAccepted.equals("Y") && hasMultipleAccepted => 20
          case tc if tc.isAccepted.equals("Y") => 10
          case _ => 30
        }
      }

      val parentSrc = if (tc.parentLsid.isEmpty) None else Some(50)
      val rankString = {
        if (tc.rank.length > 0)
          tc.rank
        else {
          //check to see if the name for the TC has a rank string
          val tn = tnDAO.getByLsid(tc.nameLsid)
          if (tn.isDefined)
            tn.get.rank
          else
            ""
        }
      }
      val rank = Ranks.matchTerm(rankString, tn.nomenCode)
      //default rankId is unranked
      val rankId = if (!rank.isEmpty) Some(rank.get.getId) else Some(9999)
      //
      //val (accepted, synonymType) = getAcceptedDetails(tc, list.size ==1)
      //generate the soundex for the taxon name
      val (gse, sse, ise) = if (rankId.isEmpty || rankId.get > 6000) generateSoundExs(tn) else (None, None, None)

      //alaDAO.addConcept(tc.lsid, Some(tc.nameLsid), tc.parentLsid, parentSrc, Some(src), accepted, rankId, synonymType,gse,sse,ise)
      val source = if(tc.lsid.startsWith("urn:lsid:biodiversity.org.au:apni")) Some("APC") else Some("AFD")
      //println(rankId)
      val alaConcept = new AlaConceptsDTO(None, tc.lsid, Some(tc.nameLsid), tc.parentLsid, parentSrc, Some(src), None, rankId, None, gse, sse, ise, source)
      //println(tc.lsid +" " + tc.nameLsid+ " " + tc.parentLsid + " " + parentSrc + " " + src + " " + accepted + " " + rankId + " " + synonymType+ " " + gse + " "+ sse +  " " + ise)
      //Some(alaConcept)
      alaDAO.insertNewTerm(alaConcept)

      //insert all the synonyms associated with this concept
      val synList = relDAO.getAllSynonymRelationships(tc.lsid)
      synList.foreach(it => {
        alaDAO.insertSynonym(tc.lsid, it._1, it._2, it._3)
      })

      None

    } else
      None

  }
  /**
   * Generate the sound expression for the supplied taxon name
   * 
   */
  def generateSoundExs(tn: TaxonNameDTO): (Option[String], Option[String], Option[String]) = {
    try {
      if (tn == null || tn.genus == null || tn.genus.length < 2)
        (None, None, None)
      else {
        (getSoundEx(tn.genus.trim, false), getSoundEx(tn.specificEpithet.trim, true), getSoundEx(tn.infraspecificEpithet.trim, true))
      }
    } catch {
      case _ => (None, None, None)
    }
  }
  /**
   * Uses the taxa match algorithm to generate a sound expression for the components of a scientific name
   */
  def getSoundEx(term: String, isSpecies: Boolean): Option[String] = {
    if (term == null || term.length < 2)
      None
    else {
      try {
        Some(TaxamatchServiceImpl.treatWord(TaxamatchServiceImpl.normalize(term), if (isSpecies) "species" else "genus"))
      } catch {
        case e: Exception => {
          e.printStackTrace;
          println(term + " can't be soundexed")
          None
        }
      }
    }
  }
  
  def generateMergedClassification(){
    val start = System.currentTimeMillis
    var finishTime = start
    var startTime = start
    println("Starting to generate merged classification ")
  }
  
  /**
   * Generates the classification for the ala accepted concepts.
   */
  def generateClassification(truncate: Boolean, dao:Classification) {
    val start = System.currentTimeMillis
    var finishTime = start
    var startTime = start
    println("Starting to generate classification ")
    //turn off the non unique indexes
    dao.disableKeys
    if (truncate)
      dao.truncate
    val roots = alaDAO.getRootConcepts

    var left = 0
    var right = left
    roots.foreach { name =>
      //get the conecpt
      // val map = new scala.collection.mutable.ListMap[String,String]
      //if(name.acceptedLsid.isEmpty){
      startTime = System.currentTimeMillis
      left = right + 1
      //println("walking hierarchy for " + name.lsid)
      right = handleClassLsid(name, 1, right + 1, Map(), dao)
      finishTime = System.currentTimeMillis
      if (right - left > 100) {
        println(name.lsid

          + ", time taken for  " + (finishTime - startTime).toFloat / 1000f
          + ", total time: " + (finishTime - start).toFloat / 60000f + " minutes")
        startTime = System.currentTimeMillis
      }
      //}

      //                else{
      //                    //add the synonym
      //                    classDAO.insertSynonym(name.lsid,name.acceptedLsid.get,name.id.get)
      //                }
      //                
    }
    //turn on the non untique indexes
    dao.enableKeys
    //classDAO.updateIds
    finishTime = System.currentTimeMillis
    println("total time: " + (finishTime - start).toFloat / 60000f + " minutes")
  }
  /**
   * a recursive method used generate the lft rgt values for the concepts to form a classification
   */
  def handleClassLsid(alaConcept: AlaConceptsDTO, currentDepth: Int, currentLeft: Int, parentMap: Map[String, String], dao:Classification): Int = {

    //get all the tc that have this one as a parent
    val children = alaDAO.getChildren(alaConcept.lsid)
    var left = currentLeft
    var right = left

    val map = new scala.collection.mutable.ListMap[String, String]
    map ++= parentMap

    //set the major rank items if necessary:
    alaConcept.rankId match {
      case Some(1000) => {
        map += ("klsid" -> alaConcept.lsid)
        map += ("kname" -> getName(alaConcept.nameLsid, alaConcept.lsid))
      }
      case Some(2000) => {
        map += ("plsid" -> alaConcept.lsid)
        map += ("pname" -> getName(alaConcept.nameLsid, alaConcept.lsid))
      }
      case Some(3000) => {
        map += ("clsid" -> alaConcept.lsid)
        map += ("cname" -> getName(alaConcept.nameLsid, alaConcept.lsid))
      }
      case Some(4000) => {
        map += ("olsid" -> alaConcept.lsid)
        map += ("oname" -> getName(alaConcept.nameLsid, alaConcept.lsid))
      }
      case Some(5000) => {
        map += ("flsid" -> alaConcept.lsid)
        map += ("fname" -> getName(alaConcept.nameLsid, alaConcept.lsid))
      }
      case Some(6000) => {
        map += ("glsid" -> alaConcept.lsid)
        map += ("gname" -> getName(alaConcept.nameLsid, alaConcept.lsid))
      }
      case Some(7000) => {
        map += ("slsid" -> alaConcept.lsid)
        map += ("sname" -> getName(alaConcept.nameLsid, alaConcept.lsid))
      }
      case _ =>
    }

    children.foreach { child =>
      right = handleClassLsid(child, currentDepth + 1, right + 1, map.toMap, dao)
    }
    if(dao == mergeclassDAO){
      //we need to grab the children from the merge table
      val mchildren = mergeDAO.getChildren(alaConcept.lsid)
      mchildren.foreach { child =>
        right = handleClassLsid(child, currentDepth + 1, right + 1, map.toMap, dao)
      }
    }

    //update the depth of the concept
    //println(lsid + " : depth : " + currentDepth + " left " + currentLeft + " right " + right+1)
    //tcDAO.update(lsid, currentDepth,currentLeft, right+1)
    val rankTerm = {
      if (alaConcept.rankId.isDefined) Ranks.getTermForId(alaConcept.rankId.get) else None
    }

    val rank = if (rankTerm.isDefined) Some(rankTerm.get.canonical) else None
    if (alaConcept.acceptedLsid.isDefined) {
      dao.insertSynonym(alaConcept.lsid, alaConcept.acceptedLsid.get, alaConcept.nameLsid.get, alaConcept.id.get, alaConcept.rankId, rank)
    } else {
      //            classDAO.insertAcceptedConcept(alaConcept.lsid,alaConcept.nameLsid.getOrElse(null),alaConcept.parentLsid.getOrElse(null),
      //                if(alaConcept.rankId.isDefined) alaConcept.rankId.get.toString else null,rank.getOrElse(null), 
      //                map.get("klsid").getOrElse(null),map.get("kname").getOrElse(null),map.get("plsid").getOrElse(null),map.get("pname").getOrElse(null),
      //                map.get("clsid").getOrElse(null),map.get("cname").getOrElse(null),map.get("olsid").getOrElse(null),map.get("oname").getOrElse(null),map.get("flsid").getOrElse(null),map.get("fname").getOrElse(null),
      //                map.get("glsid").getOrElse(null),map.get("gname").getOrElse(null),map.get("slsid").getOrElse(null),map.get("sname").getOrElse(null),left, right+1,alaConcept.id.get)
      //                

      val classification = AlaClassificationDTO(alaConcept.lsid, alaConcept.nameLsid, alaConcept.parentLsid, alaConcept.acceptedLsid,
        alaConcept.rankId, rank, map.get("klsid"), map.get("kname"), map.get("plsid"), map.get("pname"),
        map.get("clsid"), map.get("cname"), map.get("olsid"), map.get("oname"), map.get("flsid"), map.get("fname"),
        map.get("glsid"), map.get("gname"), map.get("slsid"), map.get("sname"), Some(left), Some(right + 1))

      dao.insertNewTerm(classification)
    }

    right + 1
  }
  /**
   * Retrives the name of a concept to be used in the denormalised major ranks.
   */
  def getName(nameLsid: Option[String], lsid: String): String = {

    if (nameLsid.isDefined) {
      val name = tnDAO.getByLsid(nameLsid.get)
      if (name.isDefined) {
        name.get.scientificName
      } else
        ""
    } else {
      //COL term lookup
      val name = colTcDAO.getConcept(lsid)
      if (name.isDefined) {
        name.get.scientificName
      } else
        ""
    }
  }

}

//class TaxonNameExThread(queue:BlockingQueue[TaxonNameDTO], thread: Int) extends Runnable {
//  override def run(){
//    def stop = NamesGenerator.stopInit && queue.size() == 0
//    var i =0
//    val start = System.currentTimeMillis
//    NamesGenerator.db withSession {
//      while(!stop){
//        if(queue.size>0){
//          val taxonName = queue.poll();
//          val(genEx, spEx, inSpEx) = NamesGenerator.generateSoundExs(taxonName)
//          NamesGenerator.tnDAO.updateSoundsLikeExpressions(taxonName.lsid, genEx, spEx, inSpEx)
//        }
//      }
//    }
//  }
//}

class CoLMergeThread(queue: BlockingQueue[ColConceptsDTO], thread: Int) extends Runnable {
  override def run(){
    def stop = NamesGenerator.stopInit && queue.size() == 0
    var i = 0
    val start = System.currentTimeMillis    
    NamesGenerator.db withSession {
      while (!stop) {
        if (queue.size() > 0) {
          try{
            i+=1
            val fam = queue.poll()
            //perform the action
            NamesGenerator.addMergedColFamilyConcepts(fam)
          }
          catch{
            case e: Exception => e.printStackTrace();println(e.getMessage)
          }
        }
        else {
          Thread.sleep(50)
        }
      }
    }
  }
}

/**
 * A thread that will perform all the tasks associated with loading a missing APNI concept.
 */
class ApniInitThread(queue: BlockingQueue[String], thread: Int) extends Runnable {
  override def run() {
    def stop = NamesGenerator.stopInit && queue.size() == 0
    var i = 0
    val start = System.currentTimeMillis
    NamesGenerator.db withSession {
      while (!stop) {
        if (queue.size() > 0) {
          try {
            i += 1
            val nameLsid = queue.poll()
            NamesGenerator.addApniConcept(nameLsid)
            if (i > 0 && i % 100 == 0) {
              var finishTime = System.currentTimeMillis

              println("THREAD " + thread + " processed " + i + " in " + (finishTime - start).toFloat / 60000f + " minutes")
              println("Last lsid " + nameLsid)
            }
          } catch {
            case e: Exception => println(e.getMessage)
          }
        } else {
          Thread.sleep(50)
        }
      }
    }
  }
}

class GenericQueueThread[T](queue: BlockingQueue[T], thread:Int, proc:(T => Unit)) extends Runnable {
  
  def stop = NamesGenerator.stopInit && queue.size() == 0
  override def run(){
    var i = 0
    val start = System.currentTimeMillis
    NamesGenerator.db withSession {
      while (!stop) {
        if (queue.size() > 0) {
          try {
            i += 1
            val item = queue.poll()            
            proc(item)
            if (i > 0 && i % 100 == 0) {
              var finishTime = System.currentTimeMillis
              println("THREAD " + thread + " processed " + i + " in " + (finishTime - start).toFloat / 60000f + " minutes")
            }
          } catch{
            case e: Exception => println(e.getMessage)
          }
        }
      }  
    }
  }
  
}

/**
 * A thread that will perform all the tasks associated with loading a new ala concept.  This
 * will include loading all synonyms etc
 */
class AlaInitThread(queue: BlockingQueue[TaxonNameDTO], thread: Int, proc:((TaxonNameDTO)=>Unit)) extends Runnable {
  def stop = NamesGenerator.stopInit && queue.size() == 0
  var i = 0
  val start = System.currentTimeMillis
  override def run() {
    NamesGenerator.db withSession {
      while (!stop) {
        if (queue.size() > 0) {
          try {
            i += 1
            val tn = queue.poll()
            //NamesGenerator.addConcept(tn, tn.lsid)
            proc(tn)
            if (i > 0 && i % 100 == 0) {
              var finishTime = System.currentTimeMillis

              println("THREAD " + thread + " processed " + i + " in " + (finishTime - start).toFloat / 60000f + " minutes")
            }
          } catch {
            case e: Exception => println(e.getMessage)
          }
        } else {
          Thread.sleep(50)
        }
      }
    }
  }
}

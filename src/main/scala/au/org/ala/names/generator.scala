package au.org.ala.names

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue
import au.org.biodiversity.services.taxamatch.impl.TaxamatchServiceImpl
import scala.collection.mutable.ArrayBuffer
import au.org.ala.util.OptionParser

object NamesGenerator {
  val alaDAO = new AlaConceptsJDBCDAO
  val tcDAO = new TaxonConceptJDBCDAO
  val tnDAO = new TaxonNameJDBCDAO
  val relDAO = new RelationshipJDBCDAO
  val classDAO = new AlaClassificationJDBCDAO
  val colTcDAO = new ColConceptsJDBCDAO
  val db = new BoneCPDatabase
  //indicates that the init threads should stop
  var stopInit = false
  //used to store genera
  val namecache = new org.apache.commons.collections.map.LRUMap(10000)
  private val lock : AnyRef = new Object()
  def main(args: Array[String]) {
    var all = false
    var most = false
    var lftRgtNSL = false
    var initNames =false
    var addApni=false
    var createClass=false
    var shouldAddMissingKingdoms = false
    var cleanOldDumps = false
    val parser = new OptionParser("ALA Name Generation Options") {
            opt("nsl","Preprocesses the NSL records so to determine classification depths",{lftRgtNSL=true})
            opt("init","Initialise the NSL taxon names to be used in ALA",{initNames = true})
            opt("apni","Initialise the APNI taxon names to be used in ALA",{addApni = true})
            opt("kingdoms","Adds missing CoL kingdoms ot the ALA names",{shouldAddMissingKingdoms = true})
            opt("class","Generate the final classification",{createClass = true})
            opt("all","Perform all phases in the correct order",{all = true})
            opt("most","Performs all the phase EXCEPT nsl",{most=true})
            opt("clean", "Rename theold dumps so that the mysql dumps will not fail", {cleanOldDumps=true})
        }
        
        if(parser.parse(args)){
            
          db withSession {            
            if(all || lftRgtNSL)
              generateLftRgtDepthForNSL()
            if(all || most || initNames){
              initialiseAlaConcept(true)
              fillMissingParents()
            }
            if(all || most || addApni)
              initialiseApniConcepts()
            if(all || most || shouldAddMissingKingdoms)
              addMissingColKingdoms()
            if(all || most || createClass)
              generateClassification(true)
            if(all || most || cleanOldDumps)
              prepareForDump()
          }
        }
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
    val date = new java.util.Date().getTime()
    renameFile(conFileName+".txt",conFileName+date+".txt")
    renameFile(synFileName+".txt", synFileName+date+".txt")
    renameFile(idFileName+".txt", idFileName+date+".txt")
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
      right = handleLsid(id, 1, right + 1)

    }
    var finishTime = System.currentTimeMillis
    println("total time: " + (finishTime - start).toFloat / 60000f + " minutes")
  }
  /**
   * A recursive function that will allow lft rgt value to be determined for the raw taxon_concept table
   */
  def handleLsid(lsid: String, currentDepth: Int, currentLeft: Int): Int = {

    //get all the tc that have this one as a parent
    val children = tcDAO.getChildrenIds(lsid)
    var left = currentLeft
    var right = left
    children.foreach { child =>
      right = handleLsid(child, currentDepth + 1, right + 1)
    }

    //update the depth of the concept
    //println(lsid + " : depth : " + currentDepth + " left " + currentLeft + " right " + right+1)
    tcDAO.update(lsid, currentDepth, currentLeft, right + 1)
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
      val t = new Thread(new AlaInitThread(queue, i))
      t.start
      arrayBuf += t
      i += 1
    }

    tnDAO.iterateOver(tn => {

      while (queue.size >= 490) {
        Thread.sleep(50)
      }
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
    //
    //val (accepted, synonymType) = getAcceptedDetails(tc, list.size ==1)
    //generate the soundex for the taxon name
    val (gse, sse, ise) = if (tn.isDefined && (rankId.isEmpty || rankId.get > 6000)) generateSoundExs(tn.get) else (None, None, None)

    //alaDAO.addConcept(tc.lsid, Some(tc.nameLsid), tc.parentLsid, parentSrc, Some(src), accepted, rankId, synonymType,gse,sse,ise)

    //println(rankId)
    var alaConcept = new AlaConceptsDTO(None, tc.lsid, Some(tc.nameLsid), tc.parentLsid, parentSrc, Some(src), None, rankId, None, gse, sse, ise, None)
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
            alaConcept = new AlaConceptsDTO(None, tc.lsid, Some(tc.nameLsid), Some(parConcept.get.lsid), parentSrc, Some(src), None, rankId, None, gse, sse, ise, None)
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
      //add the missing parent concept to the list of APNI concepts.
      if(parent.isEmpty && tc.parentLsid.isDefined){
        val assignedParent = tcDAO.getByLsid(tc.parentLsid.get)
        parent =addApniConcept(assignedParent.nameLsid)
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
  /**
   * Generates the classification for the ala accepted concepts.
   */
  def generateClassification(truncate: Boolean) {
    val start = System.currentTimeMillis
    var finishTime = start
    var startTime = start
    println("Starting to generate classification ")
    //turn off the non unique indexes
    classDAO.disableKeys
    if (truncate)
      classDAO.truncate
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
      right = handleClassLsid(name, 1, right + 1, Map())
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
    classDAO.enableKeys
    //classDAO.updateIds
    finishTime = System.currentTimeMillis
    println("total time: " + (finishTime - start).toFloat / 60000f + " minutes")
  }
  /**
   * a recursive method used generate the lft rgt values for the concepts to form a classification
   */
  def handleClassLsid(alaConcept: AlaConceptsDTO, currentDepth: Int, currentLeft: Int, parentMap: Map[String, String]): Int = {

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
      right = handleClassLsid(child, currentDepth + 1, right + 1, map.toMap)
    }

    //update the depth of the concept
    //println(lsid + " : depth : " + currentDepth + " left " + currentLeft + " right " + right+1)
    //tcDAO.update(lsid, currentDepth,currentLeft, right+1)
    val rankTerm = {
      if (alaConcept.rankId.isDefined) Ranks.getTermForId(alaConcept.rankId.get) else None
    }

    val rank = if (rankTerm.isDefined) Some(rankTerm.get.canonical) else None
    if (alaConcept.acceptedLsid.isDefined) {
      classDAO.insertSynonym(alaConcept.lsid, alaConcept.acceptedLsid.get, alaConcept.nameLsid.get, alaConcept.id.get, alaConcept.rankId, rank)
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

      classDAO.insertNewTerm(classification)
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
/**
 * A thread that will perform all the tasks associated with loading a new ala concept.  This
 * will include loading all synonyms etc
 */
class AlaInitThread(queue: BlockingQueue[TaxonNameDTO], thread: Int) extends Runnable {
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
            NamesGenerator.addConcept(tn, tn.lsid)
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

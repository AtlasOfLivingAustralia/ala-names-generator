package au.org.ala.names
// Import the session management, including the implicit threadLocalSession
import org.scalaquery.session._
import org.scalaquery.session.Database.threadLocalSession
import org.scalaquery.ql._
import org.scalaquery.ql.TypeMapper._
import org.scalaquery.ql.extended.MySQLDriver.Implicit._
import org.scalaquery.ql.extended.{ ExtendedTable => Table }
import au.org.ala.checklist.lucene.CBIndexSearch
import org.apache.lucene.index.IndexReader
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.search.IndexSearcher
import au.org.ala.data.util.RankType
import au.org.biodiversity.services.taxamatch.impl.TaxamatchServiceImpl
import java.io.File
import au.org.biodiversity.services.taxamatch.TaxamatchSpeciesItem
import java.util.concurrent.BlockingQueue
import java.util.concurrent.ArrayBlockingQueue
import org.scalaquery.simple.StaticQuery._
import org.scalaquery.simple.DynamicQuery

//
trait DAO {

}
trait TaxonNameDAO extends DAO {
  //def 
}
//USED

case class NamesListDTO(id:Option[Int],name:String, organisation:Option[String], contact:Option[String])

case class NamesListPaddingDTO(id:Int, taxonRank:Option[String], scientificName:Option[String], padType:String)

case class NamesListNameDTO(list_id:Int, lsid:String, acceptedLsid:Option[String], parentLsid:Option[String],
            originalLsid:Option[String], scientificName:String, publicationYear:Option[String], genus:Option[String],
            specificEpithet:Option[String], infraSpecificEpithet:Option[String], rank:Option[String], author:Option[String],
            nomenCode:Option[String], taxonomicStatus:Option[String], nomenStatus:Option[String], occurrenceStatus:Option[String], 
            genex:Option[String],spex:Option[String], inspex:Option[String])

case class ExtraNamesDTO(lsid:String,scientificName:String, authority:String, commonName:String, family:String,genus:String,specificEpithet:String)

case class AlaConceptsDTO(id: Option[Int], lsid: String, nameLsid: Option[String], parentLsid: Option[String],
  parentSrc: Option[Int], src: Option[Int], acceptedLsid: Option[String], rankId: Option[Int], synonymType: Option[Int], genusSoundEx: Option[String], speciesSoundEx: Option[String], infraSoundEx: Option[String],
  source: Option[String])

case class AlaClassificationDTO(lsid: String, nameLsid: Option[String], parentLsid: Option[String], accepetedLsid: Option[String], rankId: Option[Int], rank: Option[String],
  klsid: Option[String], kname: Option[String],
  plsid: Option[String], pname: Option[String], clsid: Option[String], cname: Option[String], olsid: Option[String], oname: Option[String],
  flsid: Option[String], fname: Option[String], glsid: Option[String], gname: Option[String], slsid: Option[String], sname: Option[String],
  lft: Option[Int], rgt: Option[Int])

case class TaxonNameDTO(lsid: String, scientificName: String, title: String,
  uninomial: String, genus: String, specificEpithet: String, subspecificEpithet: String, infraspecificEpithet: String,
  hybridForm: String, authorship: String, authorYear: String, basionymAuthor: String, rank: String,
  nomenCode: String, phraseName: String, manuscriptName: String, genex:Option[String], spex:Option[String], inspex:Option[String], unused:Option[String])

case class TaxonConceptDTO(lsid: String, rank: String, scientificName: String, nameLsid: String,
  lastModified: java.sql.Date, protologue: String, isAccepted: String, isSuperseded: Option[String], isDraft: String, parentLsid: Option[String], synonymType: Option[Int],
  acceptedLsid: Option[String], lft: Int, rgt: Int, depth: Int, isExcluded:Option[String], noAccTreeConcept:Option[String]) {

}
case class ColTaxonTreeDTO(taxonId: Int, name: String, rank: String, parentId: Int, lsid: String, numberOfChildren: Int)

case class ColConceptsDTO(taxonId: Int, lsid: String, scientificName: String, rank: String, parentId: Option[Int],
  kingdomId: Option[Int], kingdomName: Option[String], phylumId: Option[Int], phylumName: Option[String],
  classId: Option[Int], className: Option[String], orderId: Option[Int], orderName: Option[String],
  familyId: Option[Int], familyName: Option[String], genusId: Option[Int],
  genusName: Option[String], speciesId: Option[Int], speciesName: Option[String],
  infraspeciesName: Option[String], author: Option[String], nslLsid: Option[String])

case class ColSynonymsDTO(id: Int, scientificName: String, author: String, rank: String, kingdom: String, acceptedId: Int, synonymType: String, nslLsid: Option[String])

case class TaxonNameAlaConceptDTO(lsid: String, rankId: Option[Int], nameLsid: String, scientificName: String, genus: String, speciesEpithet: String, nomenCode: String)

case class RelationshipsDTO(fromLsid: String, toLsid: String, relationship: String, description: String)

case class DictionaryRelationshipDTO(id: Int, relationship: String, description: String, relType: Int)

trait ScalaQuery {

  /*
   * case class NamesListPadding(id:Int, taxonRank:Option[String], scientificName:Option[String], padType:String)
   */
  
  val NamesListPadding = new Table[NamesListPaddingDTO]("names_list_padding"){
    def id = column[Int]("id")
    def taxonRank = column[Option[String]]("taxon_rank")
    def scientificName = column[Option[String]]("scientific_name")
    def padType = column[String]("pad_type")
    
    def * = id~ taxonRank ~ scientificName ~ padType <> (NamesListPaddingDTO, NamesListPaddingDTO.unapply _)
  }
  
  val NamesListName = new Table[NamesListNameDTO]("names_list_name"){
    def listId = column[Int]("list_id")
    def lsid = column[String]("lsid")
    def acceptedLsid = column[Option[String]]("accepted_lsid")
    def parentLsid = column[Option[String]]("parent_lsid")
    def originalLsid = column[Option[String]]("original_lsid")
    def scientificName = column[String]("scientific_name")
    def publicationYear = column[Option[String]]("publication_year")
    def genus = column[Option[String]]("genus")
    def specificEpithet = column[Option[String]]("specific_epithet")
    def infraSpecificEpithet = column[Option[String]]("infraspecific_ephithet")
    def rank = column[Option[String]]("rank")
    def author = column[Option[String]]("authorship")
    def nomenCode = column[Option[String]]("nomen_code")
    def taxonomicStatus = column[Option[String]]("taxonomic_status")
    def nomenStatus = column[Option[String]]("nomenclatural_status")
    def occurrenceStatus = column[Option[String]]("occurrence_status")
    def genex = column[Option[String]]("genex")
    def spex = column[Option[String]]("spex")
    def inspex = column[Option[String]]("inspex")
    def * = listId ~ lsid ~ acceptedLsid ~ parentLsid ~ originalLsid ~ scientificName ~ publicationYear ~ genus ~ 
            specificEpithet ~ infraSpecificEpithet ~ rank ~ author ~ nomenCode ~ taxonomicStatus ~ nomenStatus ~ 
            occurrenceStatus ~ genex ~ spex ~ inspex <> (NamesListNameDTO, NamesListNameDTO.unapply _)
  }
  
  val NamesList = new Table[NamesListDTO]("names_list"){
    def id = column[Option[Int]]("id")
    def name = column[String]("name")
    def organisation = column[Option[String]]("organisation")
    def contact = column[Option[String]]("contact")
    
    def * = id ~ name ~ organisation ~ contact <> (NamesListDTO, NamesListDTO.unapply _)
  }

  val ExtraNames = new Table[ExtraNamesDTO]("extra_names"){
    def lsid = column[String]("lsid")
    def scientificName=column[String]("scientific_name")
    def authority=column[String]("authority")
    def commonName=column[String]("common_name")
    def family=column[String]("family")
    def genus = column[String]("genus")
    def specificEpithet=column[String]("specific_epithet")
    
    def * = lsid ~ scientificName ~ authority ~ commonName ~ family ~ genus ~ specificEpithet <> (ExtraNamesDTO, ExtraNamesDTO.unapply _)
  
  }
  
  val ColSynonyms = new Table[ColSynonymsDTO]("ala_names.col_synonyms") {
    def id = column[Int]("id", O.PrimaryKey)
    def scientificName = column[String]("scientific_name")
    def author = column[String]("author")
    def rank = column[String]("rank")
    def kingdom = column[String]("kingdom")
    def acceptedId = column[Int]("accepted_id")
    def synonymType = column[String]("synonym_type")
    def nslLsid = column[Option[String]]("nsl_lsid")

    def * = id ~ scientificName ~ author ~ rank ~ kingdom ~ acceptedId ~ synonymType ~ nslLsid <> (ColSynonymsDTO, ColSynonymsDTO.unapply _)

  }

  //val db = Database.forURL("jdbc:mysql://localhost/new_ala_names" ,user="root", password="password", driver = "com.mysql.jdbc.Driver")
  val db2 = Database.forURL("jdbc:mysql://localhost/col2011ac", user = "root", password = "password", driver = "com.mysql.jdbc.Driver")
  // val TaxonName = new Table[(String,String,String,String,String,String,String,String,String,String,String,String,String,String,String,String)]("taxon_name"){
  val TaxonName = new Table[TaxonNameDTO]("taxon_name") {
    def lsid = column[String]("lsid", O.PrimaryKey)
    def scientificName = column[String]("scientific_name")
    def title = column[String]("title")
    def uninomial = column[String]("uninomial")
    def genus = column[String]("genus")
    def specificEpithet = column[String]("specific_epithet")
    def subspecificEpithet = column[String]("subspecific_epithet")
    def infraspecificEpithet = column[String]("infraspecific_epithet")
    def hybridForm = column[String]("hybrid_form")
    def authorship = column[String]("authorship")
    def authorYear = column[String]("author_year")
    def basionymAuthor = column[String]("basionym_author")
    def rank = column[String]("rank")
    def nomenCode = column[String]("nomen_code")
    def phraseName = column[String]("phrase_name")
    def manuscriptName = column[String]("manuscript_name")
    def genex =column[Option[String]]("genex")
    def spex =column[Option[String]]("spex")
    def inspex =column[Option[String]]("inspex")
    def unused = column[Option[String]]("unused")

    def * = lsid ~ scientificName ~ title ~ uninomial ~ genus ~ specificEpithet ~ subspecificEpithet ~ infraspecificEpithet ~ hybridForm ~ authorship ~ authorYear ~ basionymAuthor ~ rank ~ nomenCode ~ phraseName ~ manuscriptName ~ genex ~ spex ~ inspex ~ unused <> (TaxonNameDTO, TaxonNameDTO.unapply _)

    //val columns = *.productIterator.toList.map(value => value.asInstanceOf[NamedColumn[_]].name)
  }

  val ColConcepts = new Table[ColConceptsDTO]("ala_names.col_concepts") {
    def taxonId = column[Int]("taxon_id", O.PrimaryKey)
    def lsid = column[String]("lsid")
    def scientificName = column[String]("scientific_name")
    def rank = column[String]("rank")
    def parentId = column[Option[Int]]("parent_id")
    def kingdomId = column[Option[Int]]("kingdom_id")
    def kingdomName = column[Option[String]]("kingdom_name")
    def phylumId = column[Option[Int]]("phylum_id")
    def phylumName = column[Option[String]]("phylum_name")
    def classId = column[Option[Int]]("class_id")
    def className = column[Option[String]]("class_name")
    def orderId = column[Option[Int]]("order_id")
    def orderName = column[Option[String]]("order_name")
    def familyId = column[Option[Int]]("family_id")
    def familyName = column[Option[String]]("family_name")
    def genusId = column[Option[Int]]("genus_id")
    def genusName = column[Option[String]]("genus_name")
    def speciesId = column[Option[Int]]("species_id")
    def speciesName = column[Option[String]]("species_name")
    def infraspeciesId = column[Option[Int]]("infraspecies_id")
    def infraspeciesName = column[Option[String]]("infraspecies_name")
    def author = column[Option[String]]("author")
    def nslLsid = column[Option[String]]("nsl_lsid")

    def * = taxonId ~ lsid ~ scientificName ~ rank ~ parentId ~ kingdomId ~ kingdomName ~ phylumId ~ phylumName ~ classId ~ className ~ orderId ~ orderName ~ familyId ~ familyName ~ genusId ~ genusName ~ speciesId ~ speciesName ~ infraspeciesName ~ author ~ nslLsid <> (ColConceptsDTO, ColConceptsDTO.unapply _)
  }

  //val TaxonConcept = new Table[(String,String,String,String,java.sql.Date,String,String,String,String,Int,String)]("taxon_concept"){
  val TaxonConcept = new Table[TaxonConceptDTO]("taxon_concept") {
    def lsid = column[String]("lsid", O.PrimaryKey)
    def rank = column[String]("rank")
    def scientificName = column[String]("scientific_name")
    def nameLsid = column[String]("name_lsid")
    def lastModified = column[java.sql.Date]("last_modified")
    def protologue = column[String]("protologue")
    def isAccepted = column[String]("is_accepted")
    def isSuperseded = column[Option[String]]("is_superseded")
    def isDraft = column[String]("is_draft")
    def parentLsid = column[Option[String]]("parent_lsid")
    def synonymType = column[Option[Int]]("synonym_type")
    def acceptedLsid = column[Option[String]]("accepted_lsid")
    def lft = column[Int]("lft")
    def rgt = column[Int]("rgt")
    def depth = column[Int]("depth")
    def isExcluded = column[Option[String]]("is_excluded")
    def noAccTreeConcept = column[Option[String]]("no_tree_concept")

    //define the columns that make up the select * for this table
    def * = lsid ~ rank ~ scientificName ~ nameLsid ~ lastModified ~ protologue ~ isAccepted ~ isSuperseded ~ isDraft ~ parentLsid ~ synonymType ~ acceptedLsid ~ lft ~ rgt ~ depth ~ isExcluded ~ noAccTreeConcept <> (TaxonConceptDTO, TaxonConceptDTO.unapply _)

    //val columns = *.productIterator.toList.map(value => value.asInstanceOf[NamedColumn[_]].name)

    //define the defacto FKs
    def nameFK = foreignKey("fk_tc_tn", nameLsid, TaxonName)(_.lsid)
    //      def acceptedConceptFK = foreignKey("fk_tc_ac", acceptedLsid, TaxonConcept)(_.lsid)
    //      def parentConceptFK = foreignKey("fk_tc_pc", parentLsid, TaxonConcept)(_.lsid)
  }

  //val AlaConcepts = new Table[(String, String, String,Int,Int,String)]("ala_concepts"){
  val AlaConcepts = new Table[AlaConceptsDTO]("ala_concepts") {
    def id = column[Option[Int]]("id")
    def lsid = column[String]("lsid", O.PrimaryKey)
    def nameLsid = column[Option[String]]("name_lsid")
    def parentLsid = column[Option[String]]("parent_lsid")
    def parentSrc = column[Option[Int]]("parent_src")
    def src = column[Option[Int]]("src")
    def acceptedLsid = column[Option[String]]("accepted_lsid")
    def rankId = column[Option[Int]]("rank_id")
    def synonymType = column[Option[Int]]("synonym_type")
    def genusSoundEx = column[Option[String]]("genus_sound_ex")
    def speciesSoundEx = column[Option[String]]("sp_sound_ex")
    def infraSoundEx = column[Option[String]]("insp_sound_ex")
    def source = column[Option[String]]("source")

    def * = id ~ lsid ~ nameLsid ~ parentLsid ~ parentSrc ~ src ~ acceptedLsid ~ rankId ~ synonymType ~ genusSoundEx ~ speciesSoundEx ~ infraSoundEx ~ source <> (AlaConceptsDTO, AlaConceptsDTO.unapply _)
  }
  
    val MergeAlaConcepts = new Table[AlaConceptsDTO]("merge_ala_concepts") {
    def id = column[Option[Int]]("id")
    def lsid = column[String]("lsid", O.PrimaryKey)
    def nameLsid = column[Option[String]]("name_lsid")
    def parentLsid = column[Option[String]]("parent_lsid")
    def parentSrc = column[Option[Int]]("parent_src")
    def src = column[Option[Int]]("src")
    def acceptedLsid = column[Option[String]]("accepted_lsid")
    def rankId = column[Option[Int]]("rank_id")
    def synonymType = column[Option[Int]]("synonym_type")
    def genusSoundEx = column[Option[String]]("genus_sound_ex")
    def speciesSoundEx = column[Option[String]]("sp_sound_ex")
    def infraSoundEx = column[Option[String]]("insp_sound_ex")
    def source = column[Option[String]]("source")

    def * = id ~ lsid ~ nameLsid ~ parentLsid ~ parentSrc ~ src ~ acceptedLsid ~ rankId ~ synonymType ~ genusSoundEx ~ speciesSoundEx ~ infraSoundEx ~ source <> (AlaConceptsDTO, AlaConceptsDTO.unapply _)
  }

  val AlaClassification = new Table[AlaClassificationDTO]("ala_classification") {
    def lsid = column[String]("lsid", O.PrimaryKey)
    def nameLsid = column[Option[String]]("name_lsid")
    def parentLsid = column[Option[String]]("parent_lsid")
    def acceptedLsid = column[Option[String]]("accepted_lsid")
    def rankId = column[Option[Int]]("rank_id")
    def rank = column[Option[String]]("rank")
    def klsid = column[Option[String]]("klsid")
    def kname = column[Option[String]]("kname")
    def plsid = column[Option[String]]("plsid")
    def pname = column[Option[String]]("pname")
    def clsid = column[Option[String]]("clsid")
    def cname = column[Option[String]]("cname")
    def olsid = column[Option[String]]("olsid")
    def oname = column[Option[String]]("oname")
    def flsid = column[Option[String]]("flsid")
    def fname = column[Option[String]]("fname")
    def glsid = column[Option[String]]("glsid")
    def gname = column[Option[String]]("gname")
    def slsid = column[Option[String]]("slsid")
    def sname = column[Option[String]]("sname")
    def lft = column[Option[Int]]("lft")
    def rgt = column[Option[Int]]("rgt")

    def * = lsid ~ nameLsid ~ parentLsid ~ acceptedLsid ~ rankId ~ rank ~ klsid ~ kname ~ plsid ~ pname ~ clsid ~ cname ~ olsid ~ oname ~ flsid ~ fname ~ glsid ~ gname ~ slsid ~ sname ~ lft ~ rgt <> (AlaClassificationDTO, AlaClassificationDTO.unapply _)

  }
  
  val MergeAlaClassification = new Table[AlaClassificationDTO]("merge_ala_classification") {
    def lsid = column[String]("lsid", O.PrimaryKey)
    def nameLsid = column[Option[String]]("name_lsid")
    def parentLsid = column[Option[String]]("parent_lsid")
    def acceptedLsid = column[Option[String]]("accepted_lsid")
    def rankId = column[Option[Int]]("rank_id")
    def rank = column[Option[String]]("rank")
    def klsid = column[Option[String]]("klsid")
    def kname = column[Option[String]]("kname")
    def plsid = column[Option[String]]("plsid")
    def pname = column[Option[String]]("pname")
    def clsid = column[Option[String]]("clsid")
    def cname = column[Option[String]]("cname")
    def olsid = column[Option[String]]("olsid")
    def oname = column[Option[String]]("oname")
    def flsid = column[Option[String]]("flsid")
    def fname = column[Option[String]]("fname")
    def glsid = column[Option[String]]("glsid")
    def gname = column[Option[String]]("gname")
    def slsid = column[Option[String]]("slsid")
    def sname = column[Option[String]]("sname")
    def lft = column[Option[Int]]("lft")
    def rgt = column[Option[Int]]("rgt")

    def * = lsid ~ nameLsid ~ parentLsid ~ acceptedLsid ~ rankId ~ rank ~ klsid ~ kname ~ plsid ~ pname ~ clsid ~ cname ~ olsid ~ oname ~ flsid ~ fname ~ glsid ~ gname ~ slsid ~ sname ~ lft ~ rgt <> (AlaClassificationDTO, AlaClassificationDTO.unapply _)

  }

  val Relationships = new Table[RelationshipsDTO]("relationships") {
    def fromLsid = column[String]("from_lsid")
    def toLsid = column[String]("to_lsid")
    def relationship = column[String]("relationship")
    def description = column[String]("description")

    def * = fromLsid ~ toLsid ~ relationship ~ description <> (RelationshipsDTO, RelationshipsDTO.unapply _)
  }

  val DictionaryRelationship = new Table[DictionaryRelationshipDTO]("dictionary_relationship") {
    def id = column[Int]("id")
    def relationship = column[String]("relationship")
    def description = column[String]("description")
    def relType = column[Int]("type")

    def * = id ~ relationship ~ description ~ relType <> (DictionaryRelationshipDTO, DictionaryRelationshipDTO.unapply _)

  }

  val ColTaxonTree = new Table[ColTaxonTreeDTO]("_taxon_tree") {
    def taxonId = column[Int]("taxon_id", O.PrimaryKey)
    def name = column[String]("name")
    def rank = column[String]("rank")
    def parentId = column[Int]("parent_id")
    def lsid = column[String]("lsid")
    def numberOfChildren = column[Int]("number_of_children")

    def * = taxonId ~ name ~ rank ~ parentId ~ lsid ~ numberOfChildren <> (ColTaxonTreeDTO, ColTaxonTreeDTO.unapply _)
  }

}

class NamesListPaddingJDBCDAO extends ScalaQuery {
  def getAllNamesListPadding():List[NamesListPaddingDTO]={    
    val q = for{
      nlp <- NamesListPadding
      _ <- Query orderBy (Ordering.Asc(Case when nlp.padType === "merge" then 9 when nlp.padType === "all" then 1 otherwise 5))
      _ <- Query orderBy (Ordering.Asc(nlp.id))
    } yield nlp
    
    println(q.selectStatement)
    q.list()
  }
}

class NamesListNameJDBCDAO extends ScalaQuery {
  val nameAndListQuery = for {
    Projection(listId, name) <- Parameters[Int, String]
    nln <- NamesListName if nln.listId === listId && nln.scientificName === name
  } yield nln
  
  val parentAndListQuery = for {
    Projection(listId, parentId) <- Parameters[Int,String]
    nln <- NamesListName if nln.listId === listId && nln.parentLsid === parentId
  } yield nln
  
  def insert(name:NamesListNameDTO){
    NamesListName.*.insert(name)
  }
  /**
   * retrieves the names list name entry for the name that belongs to the specified list
   */
  def getByNameAndList(listId:Int, name:String): NamesListNameDTO ={
    nameAndListQuery.first(listId, name)
  }
  
  def getByParentAndList(listId:Int, parentId:String):List[NamesListNameDTO] = {
    parentAndListQuery.list(listId, parentId)
  }
}

class NamesListJDBCDAO extends ScalaQuery{
  val listNameQuery = for {
    name <- Parameters[String]
    nl <- NamesList if nl.name === name
  } yield nl
  
  val idQuery = for {
    id <- Parameters[Int]
    nl <- NamesList if nl.id === id
  } yield nl
  /**
   * insert a newly created name list
   */
  def insert(namesList:NamesListDTO){
    NamesList.*.insert(namesList)
  }
  /**
   * get the names list with the supplied title
   */
  def getNamesList(title:String):Option[NamesListDTO] = {
    listNameQuery.firstOption(title)
  }
  /**
   * Retrieve the list by id
   */
  def getNamesListById(id:Int):Option[NamesListDTO] ={
    idQuery.firstOption(id)
  }
}

class ExtraNamesJDBCDAO extends ScalaQuery{
  def insertNewTerm(extraName: ExtraNamesDTO) {
      
    ExtraNames.*.insert(extraName)

  }
  
    def truncate() {
    updateNA("truncate table extra_names").first
  }
}

class ColSynonymsJDBCDAO extends ScalaQuery {
  val synonymsQuery = for {
    id <- Parameters[Int]
    cs <- ColSynonyms if cs.acceptedId === id
  } yield cs

  def getSynonymsForId(id: Int): List[ColSynonymsDTO] = {
    synonymsQuery.list(id)
  }

  //update the concept to store the equivalent NSL lsid
  def updateNSLRef(id: Int, nslLsid: Option[String]) = {
    (for {
      cs <- ColSynonyms if cs.id === id
    } yield cs.nslLsid).update(nslLsid)
  }
}
trait Classification{
  def insertSynonym(lsid: String, acceptedLsid: String, nameLsid: String, id: Int, rankId: Option[Int], rank: Option[String])
  def insertNewTerm(alaClassification: AlaClassificationDTO)
  def truncate()
  def disableKeys()
  def enableKeys()
}
class MergeAlaClassificationJDBCDAO extends ScalaQuery with Classification {
  override def insertSynonym(lsid: String, acceptedLsid: String, nameLsid: String, id: Int, rankId: Option[Int], rank: Option[String]){
    
  }
  override def insertNewTerm(alaClassification: AlaClassificationDTO) {

    MergeAlaClassification.*.insert(alaClassification)
  }
  override def truncate() {
    updateNA("truncate table merge_ala_classification").first
  }
  //USED
  override def disableKeys() {
    updateNA("alter table merge_ala_classification disable keys").first
  }
  //USED
  override def enableKeys() {
    updateNA("alter table merge_ala_classification enable keys").first
  }
}
//USED
class AlaClassificationJDBCDAO extends ScalaQuery with Classification {

  val familyQuery = for {
    Projection(kingdom, family) <- Parameters[String, String]
    cl <- AlaClassification if cl.kname === kingdom && cl.fname === family && cl.rankId === 5000
  } yield cl.lsid

  override def truncate() {
    updateNA("truncate table ala_classification").first
  }
  //USED
  override def disableKeys() {
    updateNA("alter table ala_classification disable keys").first
  }
  //USED
  override def enableKeys() {
    updateNA("alter table ala_classification enable keys").first
  }

  def updateIds() {
    updateNA("""update ala_concepts ac1 join ala_concepts ac2 on ac1.parent_lsid = ac2.lsid  set ac1.parent_id = ac2.id""").first

    updateNA("""update ala_classification cl join ala_concepts ac on cl.lsid = ac.lsid set cl.parent_id = ac.parent_id, cl.id = ac.id""").first

    //updateNA("""update ala_classification cl join ala_concepts ac on cl.lsid = ac.lsid set cl.id = ac.id""").first

    //updateNA("""update ala_classification cl join ala_concepts ac on cl.lsid = ac.lsid set cl.name_lsid = ac.name_lsid where cl.accepted_lsid is not null""").first

  }

  override def insertSynonym(lsid: String, acceptedLsid: String, nameLsid: String, id: Int, rankId: Option[Int], rank: Option[String]) {
    if (rank.isDefined && rankId.isDefined) {
      val sql = """insert into ala_classification(lsid, accepted_lsid, id,rank_id, rank) values (?,?,?,?,?)"""

      update[(String, String, Int, Int, String)](sql).first(lsid, acceptedLsid, id, rankId.get, rank.get)
    } else {
      val sql = """insert into ala_classification(lsid, accepted_lsid,id) values(?,?,?)"""
      update[(String, String, Int)](sql).first(lsid, acceptedLsid, id)
    }
  }

  def insertAcceptedConcept(lsid: String, nameLsid: String, parentLsid: String,
    rankId: String, rank: String, klsid: String, kname: String,
    plsid: String, pname: String, clsid: String, cname: String, olsid: String, oname: String,
    flsid: String, fname: String, glsid: String, gname: String, slsid: String, sname: String,
    lft: Int, rgt: Int, id: Int) {
    val sql = """insert into ala_classification(lsid,name_lsid,parent_lsid,rank_id,rank,
                    klsid,kname,plsid,pname,clsid,,cname,olsid,oname,flsid,fname,glsid,ganme,slsid,sname,lft,rgt,id) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
    update[(String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, Int, Int, Int)](sql).first(
      lsid, nameLsid, parentLsid, rankId, rank, klsid, kname, plsid, pname, clsid, cname, olsid, oname, flsid, fname, glsid,
      gname, slsid, sname, lft, rgt, id)
  }

  override def insertNewTerm(alaClassification: AlaClassificationDTO) {

    AlaClassification.*.insert(alaClassification)
  }

  //get a list of the genera
  def getGenera(): List[AlaClassificationDTO] = {
    (for {
      ac <- AlaClassification if ac.rankId === 6000 && ac.acceptedLsid === null.asInstanceOf[String]
    } yield ac).list
  }
  //Returns the family lsid if it exists in the current classification 
  def getFamily(kingdom: String, family: String): Option[String] = {

    val list = familyQuery.list(kingdom, family)
    if (list.size > 0)
      Some(list.head)
    else
      None
  }

  def getNSLFamilies(): List[(Option[String], Option[String])] = {
    val query = for {
      cl <- AlaClassification if cl.rankId === 5000 && cl.lsid.like("urn:lsid:biodiversity.org.au%")
    } yield cl.kname ~ cl.fname

    query.list
  }

}
//USED
class ColConceptsJDBCDAO extends ScalaQuery {
  //query to get descendants of a genus
  val genDescQuery = for {
    genusId <- Parameters[Int]
    col <- ColConcepts if col.genusId === genusId && col.rank =!= "genus"
  } yield col

  //query to get the children
  val childrenQuery = for {
    id <- Parameters[Int]
    col <- ColConcepts if col.parentId === id
  } yield col

  val childrenIdQuery = for {
    id <- Parameters[Int]
    col <- ColConcepts if col.parentId === id
  } yield col.taxonId
  
  //query to get genera for a supplied family
  val genFamilyQuery = for {
    familyId <- Parameters[Int]
    col <- ColConcepts if col.familyId === familyId && col.rank === "genus"
  } yield col

  val conceptIdQuery = for {
    id <- Parameters[Int]
    col <- ColConcepts if col.taxonId === id
  } yield col

  val conceptLsidQuery = for {
    lsid <- Parameters[String]
    col <- ColConcepts if col.lsid === lsid
  } yield col

  val generaQuery = for {
    genus <- Parameters[String]
    col <- ColConcepts if col.scientificName === genus
  } yield col

  val generaWithKingQuery = for {
    Projection(kingdom, genus) <- Parameters[String, String]
    col <- ColConcepts if col.scientificName === genus && col.kingdomName === kingdom
  } yield col

  val similarQuery = for {
    Projection(scientificName,kingdom) <- Parameters[String,String]
    col <- ColConcepts if col.scientificName === scientificName && col.kingdomName === kingdom 
  } yield col

  // query to get the genus of a sopecific name with a specfic parent
  val genFamQuery = for {
    Projection(family, genus) <- Parameters[String, String]
    col <- ColConcepts if col.scientificName === genus && col.familyName === family
  } yield col.lsid

  val updateLftRgtSQL = "update col_concepts set lft = ?, rgt = ? where taxon_id = ?"
  
  def updateLftRgt(id: Object, depth: Int, left: Int, right: Int) {
    //println("SQL Updating " + id + " " + left + " " + right)
    update[(Int, Int, Int)](updateLftRgtSQL).first(left,right,id.asInstanceOf[Int])

  }
  val q1 = for {
      tc <- ColConcepts if tc.parentId === 0
    } yield tc.taxonId
    
  def getMissingParentIds():List[Int]={
     q1.list
  }
  
  def getChildrenIds(id:Int):List[Int]={
    childrenIdQuery.list(id)
  }
  /**
   * returns the children CoL concepts for the suppied id
   */
  def getChildren(id: Int): List[ColConceptsDTO] = {
    childrenQuery.list(id)
  }

  //get the children for the supplied genera
  def getDescOfGenus(genusId: Int): List[ColConceptsDTO] = {
    genDescQuery.list(genusId)
  }
  def getGeneraForFamily(familyId: Int): List[ColConceptsDTO] = {

    genFamilyQuery.list(familyId)
  }
  //return match genera
  def getGenera(kingdom: Option[String], genus: String): List[ColConceptsDTO] = {
    if (kingdom.isDefined)
      generaWithKingQuery.list(kingdom.get, genus)
    else
      generaQuery.list(genus)
  }
  def getGenusInFamily(family: String, name: String): Option[String] = {
    genFamQuery.firstOption(family, name)
  }

  def getSimilarConcept(scientificName: String, kingdom:String): Option[ColConceptsDTO] = {

    val results = similarQuery.list(scientificName, kingdom)
    if (results.size == 1)
      Some(results.head)
    else
      None
  }
  def getConcept(id: Int): Option[ColConceptsDTO] = {
    //println(conceptIdQuery.selectStatement)
    val results = conceptIdQuery.list(id)

    if (results.size == 1)
      Some(results.head)
    else
      None
  }
  //USED
  def getConcept(lsid: String): Option[ColConceptsDTO] = {

    val results = conceptLsidQuery.list(lsid)

    if (results.size == 1)
      Some(results.head)
    else
      None
  }

  //update the concept to store the equivalent NSL lsid
  def updateNSLRef(id: Int, nslLsid: Option[String]) = {
    (for {
      col <- ColConcepts if col.taxonId === id
    } yield col.nslLsid).update(nslLsid)
  }
  
  //get all the families that need to be merged into AFD
  def getAnimalPlantFamiles():List[ColConceptsDTO]={
    (for{
      col <- ColConcepts if ((col.kingdomName === "Animalia" || col.kingdomName === "Plantae") && col.rank === "family")  
    } yield col).list
  }
  /*
   *   val q1 = for {
      Join(concept, name) <- TaxonConcept leftJoin TaxonName on (_.nameLsid is _.lsid) if name.lsid === null.asInstanceOf[String]
      _ <- Query groupBy (concept.nameLsid)
    } yield concept.nameLsid
   */
  def getSpeciesForFamily(id:Int):List[ColConceptsDTO]={
    //There is a BUG yielding col by itself in this situation: https://groups.google.com/forum/#!msg/scalaquery/rAuQWKhUOh8/iA9sNUSbIeYJ
    (for{
      Join(col,ac) <- ColConcepts leftJoin AlaConcepts on (_.lsid is _.lsid) if ac.lsid === null.asInstanceOf[String] && col.familyId === id && col.rank =!="genus"
      _ <- Query orderBy (Ordering.Asc(col.genusId))
      _ <- Query orderBy (Ordering.Asc(col.speciesId))
      _ <- Query orderBy (Ordering.Asc(col.infraspeciesId))  
    } yield col.*).list
//    (for{
//      col <- ColConcepts if col.familyId === id && col.rank =!= "genus"
//      ac <-  AlaConcepts if col.lsid ==== ac.lsid 
//      Join(col,ac) <- Relationships leftJoin ColConcepts TaxonConcept on (_.toLsid is _.nameLsid) if tc.lsid === null.asInstanceOf[String]
//        _ <- Query orderBy (Ordering.Asc(col.genusId))
//        _ <- Query orderBy (Ordering.Asc(col.speciesId))
//        _ <- Query orderBy (Ordering.Asc(col.infraspeciesId))
//    } yield col).list
  }
}

class ColTaxonTreeJDBCDAO extends ScalaQuery {
  def getEquivalentConcept(scientificName: String): Option[ColTaxonTreeDTO] = {
    db2 withSession {
      val q1 = for {
        tt <- ColTaxonTree if tt.name === scientificName
      } yield tt

      val results = q1.list
      if (results.size == 1)
        Some(results.head)
      else
        None
    }
  }
}
//USED
class TaxonNameJDBCDAO extends TaxonNameDAO with ScalaQuery {

  val taxGenNameQuery = for {
    Projection(genusName, nomen) <- Parameters[String, String]
    tn <- TaxonName if tn.scientificName === genusName && tn.nomenCode === nomen
  } yield tn

  val taxSpecNameQuery = for {
    Projection(genusName, specificEpithet, nomen) <- Parameters[String, String, String]
    tn <- TaxonName if tn.genus === genusName && tn.specificEpithet === specificEpithet && tn.nomenCode === nomen && tn.infraspecificEpithet === ""
  } yield tn

  val lsidQuery = for {
    lsid <- Parameters[String]
    tn <- TaxonName if tn.lsid === lsid
  } yield tn

    
  //query to get a taxon name only if it has been included in the ala-concepts
  val nameInConceptsQuery = for {
    Projection(genusName, nomen) <- Parameters[String, String]
    tn <- TaxonName if tn.scientificName === genusName && tn.nomenCode === nomen
    ac <- AlaConcepts if ac.nameLsid === tn.lsid
  } yield tn

  val specificNameInConceptsQuery = for {
    Projection(genusName, specificEpithet, nomen) <- Parameters[String, String, String]
    tn <- TaxonName if tn.scientificName === genusName && tn.specificEpithet === specificEpithet && tn.nomenCode === nomen
    ac <- AlaConcepts if ac.nameLsid === tn.lsid
  } yield tn
  
  val soundExGSISourceQuery = for {
    Projection(genex, spex, inex, nom) <- Parameters[String, String, String,String]
    tn <- TaxonName if tn.genex === genex && tn.spex === spex && tn.inspex === inex && tn.nomenCode === nom
  } yield tn.lsid

  val soundExGSSourceQuery = for {
    Projection(genex, spex, nom) <- Parameters[String, String,String]
    tn <- TaxonName if tn.genex === genex && tn.spex === spex && tn.inspex === null.asInstanceOf[String] && tn.nomenCode === nom
  } yield tn.lsid

  def getMatchSoundExNomen(genex: String, spex: String, inex: Option[String],nom:String):List[String]={
    inex match {
      case None => soundExGSSourceQuery.list(genex, spex, nom)
      case _=> soundExGSISourceQuery.list(genex, spex, inex.get, nom)
    }
  }
  
  //Insert the supplied sounds like expression for the supplied lsid
  def updateSoundsLikeExpressions(lsid:String, genex:Option[String], spex:Option[String], inspex:Option[String]){
    (for {
      tn <- TaxonName if tn.lsid === lsid
    } yield tn.genex ~ tn.spex ~ tn.inspex).update(genex, spex, inspex)
  }

  // returns the list of records that "has generic combination" but missingthe taxon_Concept
  def getConceptForNameSynMissingConcept(): List[(String, String)] = {
    //         val q1 = for{
    //             r <- Relationships if r.relationship === "has generic combination"
    //             tn <-  TaxonName if r.toLsid === tn.lsid 
    //             Join(r,tc) <- Relationships leftJoin TaxonConcept on (_.toLsid is _.nameLsid) if tc.lsid === null.asInstanceOf[String]
    //         } yield r
    //         
    /*
          *   val q2 = for {
    Join(c, Join(b, a)) <- C leftJoin (B leftJoin A on (_.aid === _.id)) on { case (c, Join(_,a)) => c.aid === a.id }
  } yield a.id ~ b.id ~ c.id
  
  select relationship,description, count(*) from relationships r 
  left join taxon_concept tc on  tc.name_lsid = to_lsid  where relationship <> 'excludes' and to_lsid like '%name%' and tc.name_lsid is null group by relationship, description


          */

    val q = for {
      Join(r, tc) <- Relationships leftJoin TaxonConcept on (_.toLsid is _.nameLsid) if r.relationship =!= "excludes" && r.relationship =!= "has vernacular" && r.toLsid.like("%name%") && tc.nameLsid === null.asInstanceOf[String]
      _ <- Query groupBy r.toLsid
    } yield r.fromLsid ~ r.toLsid

    //         val q1 = for{              
    //             Join(r,tc) <- Relationships leftJoin TaxonConcept on (_.toLsid is _.nameLsid) if r.relationship === "has generic combination" && tc.lsid === null.asInstanceOf[String] && r.toLsid =!= "urn:lsid:biodiversity.org.au:afd.name:490623" && r.toLsid =!="urn:lsid:biodiversity.org.au:afd.name:503821"
    //             _<- Query groupBy r.toLsid    
    //         } yield r.fromLsid ~ r.toLsid
    //         
    println(q.selectStatement)
    //Join(concept,name) <- TaxonConcept leftJoin TaxonName on (_.nameLsid is _.lsid) if name.lsid === null.asInstanceOf[String]
    q.list
    //select r.*,tn.* from relationships r  join taxon_name tn on r.to_lsid = tn.lsid left join taxon_concept tc on r.to_lsid = tc.name_lsid  where relationship = 'has generic combination' and tc.lsid is null
  }

  //def iterateOver(table:Table[_], columnsToExtract:ColumnBase[_],proc:((String, Map[String,String])=>Boolean)){
  //USED
  def iterateOver(proc: ((TaxonNameDTO) => Boolean)) {
    //db withSession{
    val q1 = for {
      tn <- TaxonName if tn.specificEpithet =!= "sp." && tn.unused === null.asInstanceOf[String] //only discounting the sp. names
    } yield tn

    //q1.map
    println(q1.selectStatement)
    q1.foreach(result => {

      proc(result)
    })

    //            val q1list = q1.list
    //            println(q1list.getClass + " " + )
    //            //q1.
    //}
  }
  //USED
  def getByLsid(lsid: String): Option[TaxonNameDTO] = {

    val list = lsidQuery.list(lsid)
    if (list.size > 0)
      Some(list.head)
    else
      None
  }

  //    def getIds():List[String]={
  //       // db withSession {
  //            val q1 = for {
  //                tn <- TaxonName
  //                tc <- TaxonConcept
  //                _ <- Query groupBy(tn.lsid)
  //            } yield tn.lsid
  //            q1.list
  //        //}
  //    }
  def getTaxonNamesForMissingParents(minRank: Option[Int], maxRank: Option[Int]): List[TaxonNameAlaConceptDTO] = {
    val q1 = for {
      ac <- AlaConcepts if ac.parentLsid === null.asInstanceOf[String] && ac.acceptedLsid === null.asInstanceOf[String] && ac.rankId >= minRank.get && ac.rankId <= maxRank.get
      tn <- TaxonName if tn.lsid === ac.nameLsid

    } yield (ac.lsid ~ ac.rankId ~ tn.lsid ~ tn.scientificName ~ tn.genus ~ tn.specificEpithet ~ tn.nomenCode) <> (TaxonNameAlaConceptDTO, TaxonNameAlaConceptDTO.unapply _)
    println(q1.selectStatement)
    q1.list
  }

  def getTaxonNameIfIncluded(genusName: String, nomen: String): Option[TaxonNameDTO] = {
    nameInConceptsQuery.firstOption(genusName, nomen)
  }
  //Only use this to get a taxon name that is identical at a rank family level or above.
  //def getIdenticalTaxonName(name:String, rank:String){
    
  //}

  def getTaxonName(genusName: String, specificEpithet: Option[String], nomen: String): List[TaxonNameDTO] = {
    specificEpithet match {
      case None => nameInConceptsQuery.list(genusName, nomen)
      case _ => specificNameInConceptsQuery.list(genusName, specificEpithet.get, nomen)
    }
  }

}
//USED
class TaxonConceptJDBCDAO extends ScalaQuery {

  val taxonGroupQuery = for {
    nameLsid <- Parameters[String]
    tc <- TaxonConcept if tc.nameLsid === nameLsid //&& tc.isAccepted =!= "N" //We only want to worry about the accepted 
    _ <- Query orderBy (Ordering.Asc(Case when tc.isAccepted === "Y" then 1 when tc.isAccepted === "N" then 100 otherwise 2))
    _ <- Query orderBy (Ordering.Desc(tc.depth))
    _ <- Query orderBy (Ordering.Desc((tc.rgt - tc.lft))) //number of children
    _ <- Query orderBy (Ordering.Desc(tc.lastModified))
  } yield tc

  val acceptedTaxonGroupQuery = for {
    nameLsid <- Parameters[String]
    tc <- TaxonConcept if tc.nameLsid === nameLsid && tc.isAccepted === "Y" && tc.isSuperseded === null.asInstanceOf[String] && tc.isExcluded === null.asInstanceOf[String] && tc.noAccTreeConcept === null.asInstanceOf[String] //We only want to worry about the accepted 
    _ <- Query orderBy (Ordering.Asc(Case when tc.isAccepted === "Y" then 1 when tc.isAccepted === "N" then 100 otherwise 2))
    _ <- Query orderBy (Ordering.Desc(tc.depth))
    _ <- Query orderBy (Ordering.Desc((tc.rgt - tc.lft))) //number of children
    _ <- Query orderBy (Ordering.Desc(tc.lastModified))
  } yield tc

  val lsidQuery = for {
    lsid <- Parameters[String]
    tc <- TaxonConcept if tc.lsid === lsid
  } yield tc

  //USED    
  def getByLsid(lsid: String): TaxonConceptDTO = {
    lsidQuery.first(lsid)
  }

  
  //USED
  /**
   * Returns a list of taxon concept that have the supplied name_lsid.  The list will be ordered according 
   * to what should be considered the accepted concept for the name.
   */
  def getConceptsForName(nameLsid: String): List[TaxonConceptDTO] = {
    taxonGroupQuery.list(nameLsid)
  }
  
  //USED
  /**
   * Returns a list of is_accepeted='T' taxon concepts for the supplied name.
   */
  def getAcceptedConceptsForName(nameLsid: String): List[TaxonConceptDTO] = acceptedTaxonGroupQuery.list(nameLsid)

  //USED
  /**
   * Returns a list of taxon concepts that don't have a parent.
   */
  def getMissingParentIds(): List[String] = {
    val q1 = for {
      tc <- TaxonConcept if tc.parentLsid === null.asInstanceOf[String]
    } yield tc.lsid
    q1.list
  }
  //USED
  /**
   * Returns a list of lsids for taxon concepts that are considered children of the supplied 
   * taxon lsid.
   */
  def getChildrenIds(lsid: String): List[String] = {
    val q1 = for {
      tc <- TaxonConcept if tc.parentLsid === lsid
    } yield tc.lsid
    q1.list
  }
  //USED
  /**
   * Updates the taxon concept with the lft, rgt values and depth.  By setting these values we
   * can order taxon concepts with name groups by the number of descedants.
   */
  def update(lsid: String, depth: Int, left: Int, right: Int) {
    val q1 = (for {
      tc <- TaxonConcept if tc.lsid === lsid
    } yield tc.depth ~ tc.lft ~ tc.rgt).update(depth, left, right)

  }
  /**
   * Returns the name lsid for taxon concepts whose name is not available in the taxon_name table.
   */
  def getNameLsidsMissingFromNames(): List[String] = {
    val q1 = for {
      Join(concept, name) <- TaxonConcept leftJoin TaxonName on (_.nameLsid is _.lsid) if name.lsid === null.asInstanceOf[String]
      _ <- Query groupBy (concept.nameLsid)
    } yield concept.nameLsid

    q1.list
  }
  /**
   * Returns all the TaxonConceptDTO objects in the supplied set of lsids. 
   */
  def getConcepts(lsids: List[String]): List[TaxonConceptDTO] = {

    val q1 = for {
      tc <- TaxonConcept if tc.lsid inSet lsids
      _ <- Query orderBy (Ordering.Desc(tc.isAccepted))
      _ <- Query orderBy (Ordering.Desc(tc.depth))
      _ <- Query orderBy (Ordering.Desc((tc.rgt - tc.lft))) //number of children
      _ <- Query orderBy (Ordering.Desc(tc.lastModified))
    } yield tc

    //println(q1.selectStatement)

    q1.list
  }
}
//USED
class RelationshipJDBCDAO extends ScalaQuery {

  val forwardRelQuery = for {
    lsid <- Parameters[String]
    r <- Relationships if r.toLsid === lsid && r.toLsid =!= r.fromLsid //don't want synonym to point to itself
    ftc <- TaxonConcept if r.fromLsid === ftc.lsid
    ttc <- TaxonConcept if r.toLsid === ttc.lsid && ttc.nameLsid =!= ftc.nameLsid //don't want synonym/accepted concept to have same namelsid
    dr <- DictionaryRelationship if dr.relationship === r.relationship && dr.description === r.description && (dr.relType === 7 || (dr.relType === 11 && ftc.isAccepted === "Y")) // rel type 11 is only valid if it points to an accepted concept            
  } yield ftc.lsid ~ dr.id

  val forwardOneQuery = for {
    lsid <- Parameters[String]
    r <- Relationships if r.toLsid === lsid && r.toLsid =!= r.fromLsid //don't want synonym to point to itself
    ftc <- TaxonConcept if r.fromLsid === ftc.lsid
    ttc <- TaxonConcept if r.toLsid === ttc.lsid && ttc.nameLsid =!= ftc.nameLsid //don't want synonym/accepted concept to have same namelsid
    dr <- DictionaryRelationship if dr.relationship === r.relationship && dr.description === r.description && (dr.relType === 7 || dr.relType === 11) // only one taxon concept for the name so we can include all the relationships            
  } yield ftc.lsid ~ dr.id

  val reverseRelQuery = for {
    lsid <- Parameters[String]
    r <- Relationships if r.fromLsid === lsid && r.toLsid =!= r.fromLsid
    ftc <- TaxonConcept if r.fromLsid === ftc.lsid
    ttc <- TaxonConcept if r.toLsid === ttc.lsid && ttc.nameLsid =!= ftc.nameLsid //don't want synonym/accepted concept to have same namelsid
    dr <- DictionaryRelationship if dr.relationship === r.relationship && dr.description === r.description && dr.relType === 8
  } yield r.toLsid ~ dr.id
  //retrieve the forward synonyms for the supplied lsid ie the from taxa is the accepted concept
  val getForwardSynonyms = for {
    lsid <- Parameters[String]
    r <- Relationships if r.fromLsid === lsid && r.fromLsid =!= r.toLsid //don't want synonym to point to itself
    ftc <- TaxonConcept if r.fromLsid === ftc.lsid
    ttc <- TaxonConcept if r.toLsid === ttc.lsid && ttc.nameLsid =!= ftc.nameLsid //don't want synonym/accepted concept to have same namelsid
    dr <- DictionaryRelationship if dr.relationship === r.relationship && dr.description === r.description && (dr.relType === 7 || dr.relType === 11)

  } yield r.toLsid ~ ttc.nameLsid ~ dr.id

  //retrieve the forward synonyms for the supplied lsid ie the to taxa is the accepted concept
  val getBackwardsSynonyms = for {
    lsid <- Parameters[String]
    r <- Relationships if r.toLsid === lsid && r.fromLsid =!= r.toLsid //don't want synonym to point to itself
    ftc <- TaxonConcept if r.fromLsid === ftc.lsid
    ttc <- TaxonConcept if r.toLsid === ttc.lsid && ttc.nameLsid =!= ftc.nameLsid //don't want synonym/accepted concept to have same namelsid
    dr <- DictionaryRelationship if dr.relationship === r.relationship && dr.description === r.description && (dr.relType === 8)

  } yield r.toLsid ~ ftc.nameLsid ~ dr.id
  //USED
  /**
   * Gets all the synonyms for the supplied taxon concept lsid.
   */
  def getAllSynonymRelationships(lsid: String): List[(String, String, Int)] = {
    val forward = getForwardSynonyms.list(lsid)
    val backwards = getBackwardsSynonyms.list(lsid)
    forward ++ backwards
  }
  /**
   * 
   * An obsolete method that retrieves a single synonym type relationship for the supplied lsid.
   * 
   * By using this we were limiting a synonym to one accepted concept.  This is NOT always the case.
   * 
   * Do not use this method to determine synonyms.
   * 
   */
  def getBestSynonymTypeRelationship(lsid: String, includeAllSyn: Boolean): Option[(String, Int)] = {

    //now search for other synonym types


    val result = if (includeAllSyn) forwardOneQuery.firstOption(lsid) else forwardRelQuery.firstOption(lsid)
    //println(list)
    if (result.isDefined)
      result
    else {
      //check to see if the concept has been superseded
      reverseRelQuery.firstOption(lsid)

    }

  }
}
class MergeAlaConceptsJBBCDAO extends ScalaQuery{
  
  val childrenQuery = for {
    lsid <- Parameters[String]
    ac <- MergeAlaConcepts if ac.parentLsid === lsid
  } yield ac
  
  def insertNewTerm(alaConcept: AlaConceptsDTO) {      
    MergeAlaConcepts.*.insert(alaConcept)
  }
  def getChildren(lsid: String): List[AlaConceptsDTO] = {  
    childrenQuery.list(lsid)
  }
  
  def deleteDuplicates(){
    val query ="""delete from merge_ala_concepts where lsid in (select lsid from ala_concepts ac where ac.lsid like '%catalogue%')"""
    updateNA(query).first
  }
}
//USED
class AlaConceptsJDBCDAO extends ScalaQuery {

  val soundExGSIQuery = for {
    Projection(genex, spex, inex) <- Parameters[String, String, String]
    ac <- AlaConcepts if ac.genusSoundEx === genex && ac.speciesSoundEx === spex && ac.infraSoundEx === inex
  } yield ac.lsid

  val soundExGSQuery = for {
    Projection(genex, spex) <- Parameters[String, String]
    ac <- AlaConcepts if ac.genusSoundEx === genex && ac.speciesSoundEx === spex && ac.infraSoundEx === null.asInstanceOf[String]
  } yield ac.lsid

  val soundExGSISourceQuery = for {
    Projection(genex, spex, inex, source) <- Parameters[String, String, String,String]
    ac <- AlaConcepts if ac.genusSoundEx === genex && ac.speciesSoundEx === spex && ac.infraSoundEx === inex && ac.source === source
  } yield ac.lsid

  val soundExGSSourceQuery = for {
    Projection(genex, spex, source) <- Parameters[String, String,String]
    ac <- AlaConcepts if ac.genusSoundEx === genex && ac.speciesSoundEx === spex && ac.infraSoundEx === null.asInstanceOf[String] && ac.source === source
  } yield ac.lsid
  
  val soundExParentSIQuery = for {
    Projection(parentId, spex, inex) <- Parameters[String, String, String]
    ac <- AlaConcepts if ac.parentLsid === parentId && ac.speciesSoundEx === spex && ac.infraSoundEx === inex
  } yield ac.lsid

  val soundExParentSQuery = for {
    Projection(parentId, spex) <- Parameters[String, String]
    ac <- AlaConcepts if ac.parentLsid === parentId && ac.speciesSoundEx === spex
  } yield ac.lsid

  val conceptForNameQuery = for {
    nameLsid <- Parameters[String]
    ac <- AlaConcepts if ac.nameLsid === nameLsid
  } yield ac

  val childrenQuery = for {
    lsid <- Parameters[String]
    ac <- AlaConcepts if ac.parentLsid === lsid
  } yield ac
  
  val blacklistedQuery = for{
    ac <- AlaConcepts if ac.rankId<7000
    tc <- TaxonConcept if ac.lsid === tc.lsid && ( tc.scientificName.like("Unplaced%") || tc.scientificName.like("Unknown%") )
  } yield ac

  val getNameParentQuery = for {
    lsid <- Parameters[String]
    tc <-TaxonConcept if tc.lsid === lsid
    ac <- AlaConcepts if ac.nameLsid === tc.nameLsid    
  } yield ac.lsid
  
  val insertSynQuery = "insert into ala_synonyms(lsid,name_lsid, accepted_lsid,syn_type) values(?,?,?,?)"
    
    
  def getUnknownUnplacedConcepts():List[AlaConceptsDTO] ={
    blacklistedQuery.list
  }
  
  val nameInConceptsQuery = for {
    Projection(genusName, nomen) <- Parameters[String, String]
    tn <- TaxonName if tn.scientificName === genusName && tn.nomenCode === nomen
    ac <- AlaConcepts if ac.nameLsid === tn.lsid
  } yield ac
    
  def getConceptBasedOnNameAndCode(genusName:String, nomen:String):Option[AlaConceptsDTO]={
    nameInConceptsQuery.firstOption(genusName, nomen)
  }
  
  //USED
  /**
   * Inserts a synonym with the supplied details into the database
   */
  def insertSynonym(acceptedLsid: String, lsid: String, nameLsid: String, synType: Int) {
    update[(String, String, String, Int)](insertSynQuery).first(lsid, nameLsid, acceptedLsid, synType)
  }
  
  def addExcludedConcepts(){
    //update the AFD concepts as excluded
    updateNA("""update ala_concepts ac, nsl_taxon_concept nc 
              set ac.excluded='Y'
              where ac.name_lsid=nc.name_lsid and nc.excluded='Y'""").first
    //add the AFD excluded names as children of the concepts that they are excluded from
    updateNA("""insert into ala_concepts(lsid, name_lsid, parent_lsid,rank_id,source,excluded) 
        select r.to_lsid,r.to_lsid,ac.lsid,9999,'AFD','T' 
        from ala_concepts ac join relationships r on ac.lsid=r.from_lsid where r.relationship='excludes' and ac.source='AFD'
        group by r.to_lsid""").first          
  }
  
  //USED 
  /**
   * Inserts the synonyms that are based on name_lsids instead of taxon concept lsid
   */
  def insertNameSynonyms() {
    updateNA("""insert into ala_synonyms(lsid, name_lsid, accepted_lsid, syn_type) select r.to_lsid,r.to_lsid,ac.lsid,dr.id 
          from ala_concepts ac join relationships r on ac.lsid = r.from_lsid 
          join dictionary_relationship dr on r.relationship = dr.relationship and r.description = dr.description where to_lsid like '%name%' and dr.type in (7,11)""").first
    
    //remove the self-referencing synonyms
    updateNA("""delete from ala_synonyms where lsid = accepted_lsid and col_id is null""").first
          
    //delete synonyms for the names that don't exist
    updateNA("""delete s FROM ala_synonyms s left join taxon_name tn on  s.name_lsid = tn.lsid where col_id is null and tn.lsid is null""").first
    
    //delete synonyms that have the same scientific name as accepted name
    updateNA("""delete s from ala_synonyms s join ala_concepts ac on s.accepted_lsid = ac.lsid join taxon_name stn on stn.lsid = s.name_lsid 
              join taxon_name atn on ac.name_lsid = atn.lsid where stn.scientific_name = atn.scientific_name""").first
//THESE are NOW being included as concepts          
//    //insert the excluded name synonyms
//    updateNA("""insert into ala_synonyms(lsid, name_lsid, accepted_lsid,syn_type) select r.to_lsid,r.to_lsid,r.from_lsid,9
//              from ala_concepts ac join relationships r on ac.lsid = r.from_lsid and r.relationship='excludes'""").first
//              
//    //insert the excluded names from the tree file
//    updateNA("""insert into ala_synonyms(lsid, name_lsid, accepted_lsid,syn_type) select nc.taxon_lsid, nc.name_lsid, ac.lsid,9 
//              from nsl_taxon_concept nc left join ala_concepts ac on nc.parent_lsid = ac.lsid where excluded='Y'""").first
  }
  //USED
  /**
   * Retrieves a list of APNI name lsids (species level or below) that are NOT covered by APC.
   */
  def getNamesLsidForApniConcepts(): List[String] = {
    val getApniQuery = """select distinct(tc.name_lsid) from taxon_concept tc left join ala_concepts ac on tc.name_lsid = ac.name_lsid left join ala_synonyms syn on tc.name_lsid = syn.name_lsid 
where tc.name_lsid like '%apni%' and syn.lsid is null and ac.lsid is null and tc.rank in ('Cultivar','Form','Species','Subform','Subspecies', 'Variety','Sub-Variety')"""
    queryNA[(String)](getApniQuery).list
  }

  //GET The COL concepts that don't have parents - this is temporary to handle missing superfamily parents. 
  def getCOLConceptsMissingParents(): List[String] = {
    val query = for {
      ac <- AlaConcepts if ac.parentLsid === null.asInstanceOf[String] && ac.acceptedLsid === null.asInstanceOf[String] && ac.rankId > 1000 && ac.lsid.like("%catalogue%")
    } yield ac.lsid
    query.list
  }
  //USED
  /**
   * Retrieves ALA Concepts that have the sound expression as supplied. Allows us to find concepts that 
   * may be identical to each other based on misspelling and/or incorrect masculine etc endings.
   */
  def getMatchingSoundEx(genex: String, spex: String, inex: Option[String]): List[String] = {
    inex match {
      case None => soundExGSQuery.list(genex, spex)
      case _ => soundExGSIQuery.list(genex, spex, inex.get)
    }
  }
  def getMatchSoundExSource(genex: String, spex: String, inex: Option[String],source:String):List[String]={
    inex match {
      case None => soundExGSSourceQuery.list(genex, spex, source)
      case _=> soundExGSISourceQuery.list(genex, spex, inex.get, source)
    }
  }
  //None,tc.lsid, Some(tc.nameLsid), tc.parentLsid, parentSrc, Some(src), accepted, rankId, synonymType, gse,sse,ise
  val addConceptSQL = "insert into ala_concepts(lsid,name_lsid,parent_lsid,parent_src,src,accepted_lsid, rank_id, synonym_type, genus_sound_ex,sp_sound_ex, insp_sound_ex) values (?,?,?,?,?,?,?,?,?,?,?)"
  //        def addConcept(lsid:String,nameLsid:Option[String], parentLsid:Option[String],parentSrc:Option[Int], src:Option[Int],acceptedLsid:Option[String], rankId:Option[Int], synonymType:Option[Int], gse:Option[String],sse:Option[String],ise:Option[String]){
  //            update[(String,String,String,Option[Int],Option[Int],String,Option[Int],Option[Int],String,String,String)](addConceptSQL).first(lsid,nameLsid.getOrElse(null),parentLsid.getOrElse(null), parentSrc,src,acceptedLsid.getOrElse(null),rankId,synonymType,gse.getOrElse(null),sse.getOrElse(null),ise.getOrElse(null))
  //            
  //        }
  //      

  def truncate() = {
    updateNA("truncate ala_concepts").first
    updateNA("truncate extra_identifiers").first
    updateNA("truncate ala_synonyms").first
  }
  /**
   * Obsolete method to insert synonyms into the ala_concepts table. Synonyms are now handled separately.
   */
  def addSynonym(lsid: String, acceptedLsid: String, colId: Int, src: Int, synonymType: Int) {
    val sql = "insert into ala_concepts(lsid, accepted_lsid,name_lsid, src, synonym_type) values(?,?,?,?,?)"
    update[(String, String, Int, Int, Int)](sql).first(lsid, acceptedLsid, colId, src, synonymType)
  }
  
  val synonymQuery = """select count(*) from ala_synonyms where lsid=?"""
  def isSynonym(lsid:String): Boolean ={
    val res =query[String,Int](synonymQuery).first(lsid)
    res >0
  }
  val nameSynonymQuery="""select count(*) from taxon_name tn join ala_synonyms s on tn.lsid = s.name_lsid where tn.scientific_name=?"""
  def isNameSynonyms(sciName:String):Boolean ={
    val res = query[String,Int](nameSynonymQuery).first(sciName)
    res >0
  }
  val sciNameAcceptedQuery ="""select count(*) from taxon_name tn join ala_concepts ac on tn.lsid = ac.name_lsid where tn.scientific_name=?"""
  def isNameAccepeted(sciName:String):Boolean ={
    val res = query[String,Int](sciNameAcceptedQuery).first(sciName)
    res>0
  }

  def getNameBasedParent(lsid:String):Option[String]={
    getNameParentQuery.firstOption(lsid)
  }
  
  val deleteConceptSQL = "delete from ala_concepts where lsid = ?"
  val insertIdentifier = "insert into extra_identifiers values(?,?)"
  val updateParentFromParentSQL = "update ala_concepts set parent_lsid = ? where parent_lsid =?"
  def removeDuplicate(lsid: String, acceptedLsid: String, lftRgt: Int) {

    update[(String)](deleteConceptSQL).first(lsid)
    update[(String, String)](insertIdentifier).first(acceptedLsid, lsid)

    if (lftRgt > 1) {
      //update the parents of the children
      update[(String, String)](updateParentFromParentSQL).first(acceptedLsid, lsid)
    }

  }

  def getDuplicateNSL(): List[(String, Option[String], String, Int)] = {
    /*   """select ac1.lsid, ac1.name_lsid, tn1.scientific_name from ala_concepts ac1 
join taxon_name tn1 on ac1.name_lsid = tn1.lsid 
join taxon_name tn2 on tn1.scientific_name = tn2.scientific_name 
join ala_concepts ac2 on tn2.lsid = ac2.name_lsid
where ac1.rank_id<6000 and ac1.lsid like '%apni%' and ac1.rank_id = ac2.rank_id and ac1.id <> ac2.id 
group by ac1.lsid, ac1.name_lsid, tn1.scientific_name""" */
    val q1 = for {
      ac1 <- AlaConcepts if ac1.rankId < 6000 && ac1.lsid.like("%biodiversity%")
      tn1 <- TaxonName if tn1.lsid === ac1.nameLsid && tn1.scientificName =!= "MACROGLOSSINAE"
      tc1 <- TaxonConcept if ac1.lsid === tc1.lsid
      tn2 <- TaxonName if tn1.scientificName === tn2.scientificName && tn1.nomenCode === tn2.nomenCode
      ac2 <- AlaConcepts if tn2.lsid === ac2.nameLsid && ac1.id =!= ac2.id && ac1.rankId === ac2.rankId // only duplicates if they are the same rank
      _ <- Query groupBy (ac1.lsid)
      _ <- Query groupBy (ac1.nameLsid)
      _ <- Query groupBy (tn1.scientificName)
      _ <- Query groupBy (tc1.rgt - tc1.lft)
      _ <- Query orderBy (Ordering.Asc(tn1.scientificName))
      _ <- Query orderBy (Ordering.Desc(tc1.isAccepted))
      _ <- Query orderBy (Ordering.Desc(tc1.rgt - tc1.lft))
    } yield ac1.lsid ~ ac1.nameLsid ~ tn1.scientificName ~ (tc1.rgt - tc1.lft)
    println(q1.selectStatement)
    q1.list
    /*
         *  _ <-  Query  orderBy(Ordering.Asc(Case when tc.isAccepted === "Y" then 1 when tc.isAccepted === "N" then 100 otherwise 2))
                _ <- Query orderBy(Ordering.Desc(tc.depth))
                _ <- Query orderBy(Ordering.Desc((tc.rgt-tc.lft)))  //number of children
                _ <- Query orderBy(Ordering.Desc(tc.lastModified))
         */
  }
  //USED    
  def getListOfMissingParents(): List[String] = {
    queryNA[(String)]("""select distinct ac1.parent_lsid from ala_concepts ac1 left join ala_concepts ac2 on ac1.parent_lsid = ac2.lsid where ac1.parent_lsid is not null and ac2.lsid is null""").list
  }

  def addMissingGenus(parentLsid: String, genusLsid: String) {
    val sql = """insert into ala_concepts select null,null, c.lsid,null,case when c.rank='genus' then ? else p.lsid end,90,130,null,
case c.rank when 'kingdom' then 1000 when 'phylum' then 2000 when 'class' then 3000 when 'order' then 4000 
when 'family' then 5000 when 'genus' then 6000 when 'species' then 7000 
when 'form' then 8020 when 'subspecies' then 8000 when 'variety' then 8010 when 'cultivar' then 8050 
when 'race' then 8120 when 'sub-variety' then 8015  else null end,
null,null,null,null,null,null,null,c.taxon_id
from col_concepts c left join col_concepts p on c.parent_id = p.taxon_id
left join ala_concepts ac on c.lsid = ac.lsid
where c.genus_lsid = ? and  ac.lsid is null and c.nsl_lsid is null"""
    update[(String, String)](sql).first(parentLsid, genusLsid)
  }

  def addMissingDescendantsForFamily(src: Int, familyLsid: String, nomen: String) {

    val sql = """insert into ala_concepts select c.lsid,null,p.lsid,90,?,null,
case c.rank when 'kingdom' then 1000 when 'phylum' then 2000 when 'class' then 3000 when 'order' then 4000 
when 'family' then 5000 when 'genus' then 6000 when 'species' then 7000 
when 'form' then 8020 when 'subspecies' then 8000 when 'variety' then 8010 when 'cultivar' then 8050 
when 'race' then 8120 when 'sub-variety' then 8015  else null end,
null,null,null,null,null,null,null,null
from col_concepts c left join col_concepts p on c.parent_id = p.taxon_id
left join ala_concepts ac on c.lsid = ac.lsid
left join taxon_name tn on c.genus_name = tn.scientific_name and tn.nomen_code = ?
left join ala_concepts nac on tn.lsid = nac.name_lsid
where c.family_lsid =?  and c.lsid <> c.family_lsid and ac.lsid is null and c.nsl_lsid is null and nac.lsid is null group by c.lsid"""

    //println(sql  + " "+src + " " + nomen + " " +familyLsid )

    update[(Int, String, String)](sql).first(src, nomen, familyLsid)
    //println(sql)
    //updateNA(sql).first
  }
  
  //USED
  def addColSynonyms() {
    updateNA("""insert into ala_synonyms(lsid,name_lsid,accepted_lsid, col_id, syn_type) 
          select CONCAT_WS('/','species/id',col.accepted_id,'synonym',col.id),col.id, ac.lsid,col.id,d.id 
          from col_synonyms col join ala_concepts ac on ac.col_id = col.accepted_id join dictionary_relationship d 
          on col.synonym_type = d.relationship and d.description='COL'""").first
  }
  //USED
  def addMissingConceptsFromCoL() {
    //add the Viruses

    //There are 2 CoL families in different areas of the tree that share an LSID so concat the id as well for the following id's: (2348007,2349727)
    updateNA("""insert into ala_concepts select distinct null,null, case when c.taxon_id in (2348007,2349727) then concat_ws('|',c.lsid,c.taxon_id) else c.lsid end,null,case when p.taxon_id in (2348007,2349727) then concat_ws('|',p.lsid,p.taxon_id) else p.lsid end,90,110,null,
case c.rank when 'kingdom' then 1000 when 'phylum' then 2000 when 'class' then 3000 when 'order' then 4000 
when 'family' then 5000 when 'genus' then 6000 when 'species' then 7000 
when 'form' then 8020 when 'subspecies' then 8000 when 'variety' then 8010 else null end ,
null,null,null,null,null,null,null,c.taxon_id,'COL',null
from col_concepts c left join col_concepts p on c.parent_id = p.taxon_id where c.kingdom_name in ('Viruses','Chromista','Protozoa','Bacteria','Fungi')""").first

    //Now remove reference to Unassigned values
    updateNA("""update ala_concepts ac1 ,ala_concepts ac2, col_concepts col  set ac1.parent_lsid = ac2.parent_lsid
where (ac1.src =110 or ac1.src=120 or ac1.src=130) and ac2.rank_id=2000 and ac1.parent_lsid = ac2.lsid and ac2.lsid = col.lsid and col.scientific_name='Not assigned'""").first

    updateNA("""update ala_concepts ac1 ,ala_concepts ac2, col_concepts col  set ac1.parent_lsid = ac2.parent_lsid
where (ac1.src =110 or ac1.src=120 or ac1.src=130) and ac2.rank_id=3000 and ac1.parent_lsid = ac2.lsid and ac2.lsid = col.lsid and col.scientific_name='Not assigned'""").first

    updateNA("""update ala_concepts ac1 ,ala_concepts ac2, col_concepts col  set ac1.parent_lsid = ac2.parent_lsid
where (ac1.src =110 or ac1.src=120 or ac1.src=130) and ac2.rank_id=4000 and ac1.parent_lsid = ac2.lsid and ac2.lsid = col.lsid and col.scientific_name='Not assigned'""").first

    updateNA("""update ala_concepts ac1 ,ala_concepts ac2, col_concepts col  set ac1.parent_lsid = ac2.parent_lsid
where (ac1.src =110 or ac1.src=120 or ac1.src=130) and ac2.rank_id=5000 and ac1.parent_lsid = ac2.lsid and ac2.lsid = col.lsid and col.scientific_name='Not assigned'""").first

    updateNA("""update ala_concepts ac1 ,ala_concepts ac2, col_concepts col  set ac1.parent_lsid = ac2.parent_lsid
where (ac1.src =110 or ac1.src=120 or ac1.src=130) and ac2.rank_id=6000 and ac1.parent_lsid = ac2.lsid and ac2.lsid = col.lsid and col.scientific_name='Not assigned'""").first

    updateNA("""update ala_concepts ac1 ,ala_concepts ac2, col_concepts col  set ac1.parent_lsid = ac2.parent_lsid
where (ac1.src =110 or ac1.src=120 or ac1.src=130) and ac2.rank_id=5000 and ac1.parent_lsid = ac2.lsid and ac2.lsid = col.lsid and col.scientific_name='Not assigned'""").first

    updateNA("""update ala_concepts ac1 ,ala_concepts ac2, col_concepts col  set ac1.parent_lsid = ac2.parent_lsid
where (ac1.src =110 or ac1.src=120 or ac1.src=130) and ac2.rank_id=7000 and ac1.parent_lsid = ac2.lsid and ac2.lsid = col.lsid and col.scientific_name='Not assigned'""").first

    updateNA("""delete ac from ala_concepts ac join col_concepts cc on ac.lsid = cc.lsid where (ac.src = 110 or ac.src=120 or ac.src=130) and cc.scientific_name='Not assigned'""").first

  }

 
  /**
   * Updates the reference so that each concept has a valid non-cyclic parent.
   */
  def updateMissingReferences = {
    //remove parents for kingdoms
    updateNA("""update ala_concepts set parent_lsid=null where rank_id=1000""").first

    //update the parent lsid when a concept points to a a taxon_concept that is not in ala_concepts
    updateNA("""update ala_concepts a, (select c.lsid as clsid, pname.lsid as plsid from ala_concepts c left join ala_concepts p on c.parent_lsid = p.lsid join taxon_concept tc on tc.lsid = c.parent_lsid join ala_concepts pname on tc.name_lsid = pname.name_lsid  where p.lsid is null) as vals
SET a.parent_lsid = vals.plsid, a.parent_src=60 where a.lsid = vals.clsid;
""").first

    //set the parent for a concepts based on the parents from the same name group
    updateNA(""" update ala_concepts ac ,
(select c.lsid as clsid ,pac.lsid as plsid from ala_concepts c join taxon_concept tc on c.name_lsid = tc.name_lsid  
join taxon_concept ptc on tc.parent_lsid = ptc.lsid join ala_concepts pac on ptc.name_lsid = pac.name_lsid 
where c.parent_lsid is null and tc.parent_lsid is not null  group by c.lsid order by c.lsid, tc.depth desc, (tc.rgt-tc.lft) desc) res
SET ac.parent_lsid = res.plsid , ac.parent_src=70 where ac.lsid = res.clsid """).first

    //Update the accepted concept to point to a concept that exists in ala_concepts
    updateNA(""" update ala_concepts a, (select sc.lsid as slsid, aname.lsid as alsid from ala_concepts sc left join ala_concepts ac on sc.accepted_lsid = ac.lsid join taxon_concept tc on sc.accepted_lsid = tc.lsid 
join ala_concepts aname on tc.name_lsid = aname.name_lsid where ac.lsid is null and sc.accepted_lsid is not null) vals
SET a.accepted_lsid = vals.alsid where a.lsid=vals.slsid """).first

    //remove all parents for the synonyms
    updateNA("""update ala_concepts set parent_lsid = null where parent_lsid is not null and accepted_lsid is not null""").first

    //remove self parents
    updateNA("""update ala_concepts set parent_lsid = null where lsid = parent_lsid""").first

    //add identifiers to the table where a synonym has an identical name to its accepted concept
    updateNA("""insert into extra_identifiers select aac.lsid,sac.lsid
from ala_concepts aac 
join ala_concepts sac on aac.lsid = sac.accepted_lsid 
join taxon_name atn on aac.name_lsid = atn.lsid
join taxon_name stn on sac.name_lsid = stn.lsid
where atn.scientific_name = stn.scientific_name""").first

    //remove the synonyms that have identical text to the accepted concept
    updateNA("""delete ac2 from ala_concepts ac1,ala_concepts ac2,taxon_name tn1,taxon_name tn2 where  ac1.lsid = ac2.accepted_lsid 
and  ac1.name_lsid = tn1.lsid 
and  ac2.name_lsid = tn2.lsid
and tn1.scientific_name = tn2.scientific_name""").first

    //update the children records for the above synonyms - so that we don't lose concepts
    updateNA("""update ala_concepts ac1 left join ala_concepts ac2 on ac1.parent_lsid = ac2.lsid join relationships r on ac1.parent_lsid = r.to_lsid  
set ac1.parent_lsid = r.from_lsid
where ac1.parent_lsid is not null and ac2.lsid is null and r.relationship = 'includes'""").first


  }

  //get the species "sounds like" 
  def getSoundsLike(parentlsid: Option[String], spex: Option[String], inex: Option[String]): Option[String] = {
    val q1 = if (inex.isDefined) {
      for {
        ac <- AlaConcepts if ac.parentLsid === parentlsid && ac.speciesSoundEx === spex && ac.infraSoundEx === inex
      } yield ac.lsid
    } else {
      for {
        ac <- AlaConcepts if ac.parentLsid === parentlsid && ac.speciesSoundEx === spex
      } yield ac.lsid
    }

    //println(q1.selectStatement)
    val results = q1.list
    if (results.size > 0)
      Some(results.head)
    else
      None
  }

  // get a list of the CoL terms
  def getColLsids(): List[String] = {
    (for {
      ac <- AlaConcepts if ac.lsid.like("%catalogue%") //ac.src === 40
    } yield ac.lsid).list
  }

  // get all the children for the concept
  //USED
  def getChildren(lsid: String): List[AlaConceptsDTO] = {
    //        (for{
    //           ac <-AlaConcepts if ac.parentLsid === lsid 
    //        } yield ac).list
    childrenQuery.list(lsid)
  }

  def updateParent(lsid: String, parentLsid: Option[String], parentSrc: Option[Int]) = {
    (for {
      ac <- AlaConcepts if ac.lsid === lsid
    } yield ac.parentLsid ~ ac.parentSrc).update(parentLsid, parentSrc)
  }
  def updateChidrenParentRefs(oldParent:Option[String], newParent:Option[String]) ={
    (for {
      ac <- AlaConcepts if ac.parentLsid === oldParent
    } yield ac.parentLsid ~ ac.parentSrc).update(newParent, Some(999))
  }
  def deleteConcept(lsid:String){
    update[(String)](deleteConceptSQL).first(lsid)
  }
  //USED
  /**
   * Returns the first ala concept for the supplied name lsid
   */
  def getAlaConceptForName(nameLsid: String): Option[AlaConceptsDTO] = {
    conceptForNameQuery.firstOption(nameLsid)           
  }
  
  //USED
  /**
   * Retrieves all the taxon concepts that do not have parents.  These are the "roots" of the classification tree.
   */
  def getRootConcepts(): List[AlaConceptsDTO] = {
    val q1 = (for {
      ac <- AlaConcepts if ac.parentLsid === null.asInstanceOf[String] // && ac.acceptedLsid =!= null.asInstanceOf[String]
    } yield ac)
    println(q1.selectStatement)
    q1.list

  }

  def getCoLSpeciesConcepts(): List[(String, Int)] = {
    //TODO change this to use col_id in ala_concepts instead of performing the joins
    val query = for {
      //col <- ColConcepts 
      //ac <- AlaConcepts if ac.lsid === col.lsid && ac.rankId >=7000
      Join(ac, col) <- AlaConcepts innerJoin ColConcepts on (_.lsid is _.lsid) if (ac.rankId >= 7000)

    } yield col.lsid ~ col.taxonId
    println(query.selectStatement)
    query.list
  }
  /**
   * Returns a list of taxon concept lsid's that reference parents that don't exist in the ala_concepts table. 
   */
  def getMissingParentIds(): List[String] = {
    val q1 = for {
      Join(concept, parent) <- AlaConcepts leftJoin AlaConcepts on (_.parentLsid is _.lsid) if parent.lsid === null.asInstanceOf[String]
    } yield concept.lsid
    q1.list
  }

  def insertBatch(batch: List[AlaConceptsDTO]) {
    AlaConcepts.*.insertAll(batch: _*)
  }


  //USED
  /**
   * Inserts the supplied alaConcepts into the database
   */
  def insertNewTerm(alaConcept: AlaConceptsDTO) {
      
    AlaConcepts.*.insert(alaConcept)

  }

}



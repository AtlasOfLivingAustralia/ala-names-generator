package au.org.ala.names

import org.gbif.dwc.text.{StarRecord, ArchiveFactory}
import java.io.File
/**
 * A singleton class that can be used to load a list of names supplied in an archive 
 * 
 * @author Natasha Quimby (natasha.quimby@csiro.au)
 */
object DwCANamesLoader {
  def namesListDAO = new NamesListJDBCDAO
  def namesListNameDAO = new NamesListNameJDBCDAO
  def loadArchive(fileName:String){
    val archive = ArchiveFactory.openArchive(new File(fileName))
    val metadata = archive.getMetadata()
    //extract the values that need to be inserted into the names_list table
    val nldto = new NamesListDTO(None, metadata.getTitle(), Some(metadata.getPublisherName()), Some(metadata.getCreatorName()))
    namesListDAO.insert(nldto)
    val namesList = namesListDAO.getNamesList(metadata.getTitle())
    if(namesList.isDefined){
        //now process the archive file
        val iter = archive.iterator
        var count=0
        while(iter.hasNext()){
          count+=1
          loadRecord(iter.next(),namesList.get.id.get, count)
        }
    } else{
      println("Error inserting names list...")
    }
    
  }
  def loadRecord(star:StarRecord, id:Int, processedCount:Int){
    //need to generate and insert "soundex" expressions for genus, specificEpithet and infraspecificEpithet
    val colDuplicates = List("urn:lsid:catalogueoflife.org:taxon:3b8d202c-60b7-102d-be47-00304854f810:col20120124","urn:lsid:catalogueoflife.org:taxon:3b8dd918-60b7-102d-be47-00304854f810:col20120124","urn:lsid:catalogueoflife.org:taxon:3b8e6072-60b7-102d-be47-00304854f810:col20120124","urn:lsid:catalogueoflife.org:taxon:e1bfeb69-2dc5-11e0-98c6-2ce70255a436:col20120124","urn:lsid:catalogueoflife.org:taxon:e38b08af-2dc5-11e0-98c6-2ce70255a436:col20120124")
    var lsid = star.core.value(org.gbif.dwc.terms.DwcTerm.taxonID)
    if(lsid == null){
      //set the lsid to the id for the record taxonID is not supplied as a separate term
      lsid = star.core.id()
    }    
    val acceptedLsid = star.core.value(org.gbif.dwc.terms.DwcTerm.acceptedNameUsageID)
    val parentLsid = star.core.value(org.gbif.dwc.terms.DwcTerm.parentNameUsageID)
    val originalLsid = star.core.value(org.gbif.dwc.terms.DwcTerm.originalNameUsageID)
    val scientificName = stripStrayQuotes(star.core.value(org.gbif.dwc.terms.DwcTerm.scientificName))
    val publicationYear = star.core.value(org.gbif.dwc.terms.DwcTerm.namePublishedInYear)
    val genus = star.core.value(org.gbif.dwc.terms.DwcTerm.genus)
    val specificEpithet = star.core.value(org.gbif.dwc.terms.DwcTerm.specificEpithet)
    val infraspecificEpithet = star.core.value(org.gbif.dwc.terms.DwcTerm.infraspecificEpithet)
    val rank = star.core.value(org.gbif.dwc.terms.DwcTerm.taxonRank)
    val author = star.core.value(org.gbif.dwc.terms.DwcTerm.scientificNameAuthorship)
    val nomenCode = star.core.value(org.gbif.dwc.terms.DwcTerm.nomenclaturalCode)
    val taxonomicStatus = star.core.value(org.gbif.dwc.terms.DwcTerm.taxonomicStatus)
    val nomenStatus = star.core.value(org.gbif.dwc.terms.DwcTerm.nomenclaturalStatus)
    val occurrenceStatus = star.core.value(org.gbif.dwc.terms.DwcTerm.occurrenceStatus)
    val genex = NamesGenerator.getSoundEx(genus, false)
    val spex = NamesGenerator.getSoundEx(specificEpithet, true)
    val inspex = NamesGenerator.getSoundEx(infraspecificEpithet, true)

    val name = new NamesListNameDTO(id, lsid,
        convertToOption(acceptedLsid),convertToOption(parentLsid),
        convertToOption(originalLsid), scientificName, 
        convertToOption(publicationYear), convertToOption(genus),
        convertToOption(specificEpithet), convertToOption(infraspecificEpithet),
        convertToOption(rank), convertToOption(author), convertToOption(nomenCode), 
        convertToOption(taxonomicStatus), convertToOption(nomenStatus),
        convertToOption(occurrenceStatus),genex, spex, inspex)
    namesListNameDAO.insert(name)
    if(processedCount%1000 == 0){
      println("Loaded " + processedCount + " - " + new java.util.Date())
    }
  }
  def convertToOption(value:String):Option[String] = if(value == null)None else Some(value.trim)
  
  def stripStrayQuotes(value:String):String= if(value != null && value.startsWith("\"") && value.endsWith("\"")) value.dropRight(1).drop(1).trim else value
  
}
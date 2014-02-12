This file outlines the steps that need to be performed to generate an ALA names list.

Some of the steps are manual.  It may be nice to incorporate them into a complete package.

1) Download the latest CSV files from http://biodiversity.org.au/dataexport/.  Save them in the /data/bie-staging/anbg directory

2) Rename the files - stripping out the date and timestamp information from the file names.

3) Run the ala-names-setup.sql script to reset the mysql environment:
mysql -uroot -ppassword< ala-names-setup.sql

4) If the CoL name staging are not populated run the import-col-names.sql script.

5) Import each of the DwC archives to be used as additional names sources eg: --dwc /data/bie-staging/names-lists/dwc-col

6) Decide how each of the DwC archives aregoing to be used in padding out the NSL.  At the moment this is a manual step that requires some SQL to be executed defining the type and level at which padding occurrs.
Based on names_list 1 = AusMoss 2 = AusFungi and 3 = CoL here is the latest SQL that was executed to populate it:
INSERT INTO names_list_padding (id,taxon_rank,scientific_name,pad_type) VALUES (1,'class','Equisetopsida','all');
INSERT INTO names_list_padding (id,taxon_rank,scientific_name,pad_type) VALUES (2,'kingdom','Chromista','all');
INSERT INTO names_list_padding (id,taxon_rank,scientific_name,pad_type) VALUES (2,'kingdom','Fungi','all');
INSERT INTO names_list_padding (id,taxon_rank,scientific_name,pad_type) VALUES (2,'kingdom','Protozoa','all');
INSERT INTO names_list_padding (id,taxon_rank,scientific_name,pad_type) VALUES (3,'kingdom','Chromista','merge');
INSERT INTO names_list_padding (id,taxon_rank,scientific_name,pad_type) VALUES (3,'kingdom','Fungi','merge');
INSERT INTO names_list_padding (id,taxon_rank,scientific_name,pad_type) VALUES (3,'kingdom','Protozoa','merge');
insert into names_list_padding(id,taxon_rank,pad_type) VALUES (1, 'kingdom', 'all');
insert into names_list_padding(id,taxon_rank,pad_type) VALUES (2, 'kingdom', 'all');
INSERT INTO names_list_padding (id,taxon_rank,scientific_name,pad_type) VALUES (3,'order','Diphysciales','merge');
INSERT INTO names_list_padding (id,taxon_rank,scientific_name,pad_type) VALUES (3,'order','Archidiales','merge');
INSERT INTO names_list_padding (id,taxon_rank,scientific_name,pad_type) VALUES (3,'order','Hypnales','merge');
INSERT INTO names_list_padding (id,taxon_rank,scientific_name,pad_type) VALUES (3,'order','Scouleriales','merge');
INSERT INTO names_list_padding (id,taxon_rank,scientific_name,pad_type) VALUES (3,'order','Bartramiales','merge');
INSERT INTO names_list_padding (id,taxon_rank,scientific_name,pad_type) VALUES (3,'order','Encalyptales','merge');
INSERT INTO names_list_padding (id,taxon_rank,scientific_name,pad_type) VALUES (3,'order','Pottiales','merge');
INSERT INTO names_list_padding (id,taxon_rank,scientific_name,pad_type) VALUES (3,'order','Buxbaumiales','merge');
INSERT INTO names_list_padding (id,taxon_rank,scientific_name,pad_type) VALUES (3,'order','Hedwigiales','merge');
INSERT INTO names_list_padding (id,taxon_rank,scientific_name,pad_type) VALUES (3,'order','Hookeriales','merge');
INSERT INTO names_list_padding (id,taxon_rank,scientific_name,pad_type) VALUES (3,'order','Ptychomniales','merge');
INSERT INTO names_list_padding (id,taxon_rank,scientific_name,pad_type) VALUES (3,'order','Splachnales','merge');
INSERT INTO names_list_padding (id,taxon_rank,scientific_name,pad_type) VALUES (3,'order','Gigaspermales','merge');
INSERT INTO names_list_padding (id,taxon_rank,scientific_name,pad_type) VALUES (3,'order','Sphagnales','merge');
INSERT INTO names_list_padding (id,taxon_rank,scientific_name,pad_type) VALUES (3,'order','Orthotrichales','merge');
INSERT INTO names_list_padding (id,taxon_rank,scientific_name,pad_type) VALUES (3,'order','Andreaeales','merge');
INSERT INTO names_list_padding (id,taxon_rank,scientific_name,pad_type) VALUES (3,'order','Polytrichales','merge');
INSERT INTO names_list_padding (id,taxon_rank,scientific_name,pad_type) VALUES (3,'order','Bryales','merge');
INSERT INTO names_list_padding (id,taxon_rank,scientific_name,pad_type) VALUES (3,'order','Grimmiales','merge');
INSERT INTO names_list_padding (id,taxon_rank,scientific_name,pad_type) VALUES (3,'order','Funariales','merge');
INSERT INTO names_list_padding (id,taxon_rank,scientific_name,pad_type) VALUES (3,'order','Hypnodendrales','merge');
INSERT INTO names_list_padding (id,taxon_rank,scientific_name,pad_type) VALUES (3,'order','Rhizogoniales','merge');
INSERT INTO names_list_padding (id,taxon_rank,scientific_name,pad_type) VALUES (3,'order','Dicranales','merge');
INSERT INTO names_list_padding (id,taxon_rank,scientific_name,pad_type) VALUES (3,'kingdom','Viruses','all');
INSERT INTO names_list_padding (id,taxon_rank,scientific_name,pad_type) VALUES (3,'kingdom','Bacteria','all');

6) To generate the names list in the mysql database execute the following command:
java -Xmx1G -Xms1G -cp .:ala-names-generator-1.0-SNAPSHOT-assembly.jar au.org.ala.names.NamesGenerator --all

This command performs the following individual steps (each of these steps can be run separately using the corresponding argument.
a) --tnexp : generates the sound exp to be used to assist in the potential duplicates. If a sound exp (taxa specific to account for masculine/feminine) - 9.83255 minutes 
b) --nsl : Calculates the depth and number of children for the nsl taxon concepts.  This aids in the selection of an "accepted" concept. - total time: 38.595684 minutes (886,982 taxon concepts)
c) --init : Extracts the "Accepted" AFD and APC taxon concepts from the NSL.  It also associates synonyms for the concepts - 34 minutes
d) --caab : Inserts the missing names from a subset on caab names.  This is not available as a DwCA thus needs specialised attention
e) --synexp: Populates the sound like expressions for the synonyms. These are used to determine whether or not to include
f) --lists all : Incorporates the names lists that should have "all" their concepts included
g) --lists merge : Incorporates the names lists that should have their concepts "merge"d with the existing concepts.  This step needs to be performed after the --lists all to take into account concepts added in that step.
h) --apni : Extracts the APNI species level and below concepts for use in the names list.
i) --col : pads out AFD with concepts that have records in Australia  
j) --class: Generates the classification for all the ALA accepted concepts. This includes a denormalised view of the major linnaean ranks.
k) --clean: Renames the mysql names dumps so that there is no conflict in file name


o) --kingdoms: Incorporates all the CoL kingdoms that are missing from the NSL. This includes synonyms. (This step is obsolete/deprecated since support of the dwc lists) 

7) Dump the ALA accepted concepts and synonyms to file. 
mysql -uroot -ppassword < create-dumps.sql

This file outlines the steps that need to be performed to generate an ALA names list.

Some of the steps are manual.  It may be nice to incorporate them into a complete package.

1) Download the latest CSV files from http://biodiversity.org.au/dataexport/.  Save them in the /data/bie-staging/anbg directory

2) Rename the files - stripping out the date and timestamp information from the file names.

3) Run the ala-names-setup.sql script to reset the mysql environment:
mysql -uroot -ppassword< ala-names-setup.sql

4) If the CoL name staging are not populated run the import-col-names.sql script.

5) To generate the names list in the mysql database execute the following command:
java -Xmx1G -Xms1G -cp .:ala-names-generator-1.0-SNAPSHOT-assembly.jar au.org.ala.names.NamesGenerator --all

This command performs the following individual steps (each of these steps can be run separately using the corresponding argument.

a) --nsl : Calculates the depth and number of children for the nsl taxon concepts.  This aids in the selection of an "accepted" concept.
b) --init: Extracts the "Accepted" AFD and APC taxon concepts from the NSL.  It also associates synonyms for the concepts
c) --apni: Extracts the APNI species level and below concepts for use in the names list.
d) --kingdoms: Incorporates all the CoL kingdoms that are missing from the NSL. This includes synonyms.
e) --class: Generates the classification for all the ALA accepted concepts. This includes a denormalised view of the major linnaean ranks.
f) --clean: Renames the mysql names dumps so that there is no conflict in file name

6) Dump the ALA accepted concepts and synonyms to file. 
mysql -uroot -ppassword < create-dumps.sql

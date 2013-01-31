	-- create the database
	CREATE DATABASE IF NOT EXISTS ala_names
  	DEFAULT CHARACTER SET utf8
 	DEFAULT COLLATE utf8_general_ci;

  	use ala_names;

	DROP TABLE IF EXISTS taxon_concept;
	
	create table taxon_concept(
	lsid varchar(255),
	rank varchar(50),
	scientific_name varchar(500),
	name_lsid varchar(255),
	last_modified date,
	protologue char(1),
	is_accepted char(1),
	is_draft char(1),
	parent_lsid varchar(255),
	synonym_type int,
	accepted_lsid varchar(1000),
	lft int,
	rgt int,
	depth int,
	is_superseded char(1),
	is_excluded char(1),
	no_tree_concept char(1),
	primary key(lsid),
	index ix_tc_name_lsid(name_lsid),
	index ix_tc_parent(parent_lsid)
	);

	load data infile '/data/bie-staging/anbg/ALA_AFD_TAXON.csv' 
	IGNORE INTO table taxon_concept CHARACTER SET 'utf8'
	FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' IGNORE 1 LINES
	(lsid, @ignore, rank, scientific_name, name_lsid, @last_modified, @ignore, protologue, @ignore, is_accepted, is_draft)
	SET last_modified = case when @last_modified = '' then null else str_to_date(@last_modified, '%d-%m-%Y') end;
	
	load data infile '/data/bie-staging/anbg/ALA_APNI_TAXON.csv' 
	IGNORE INTO table taxon_concept CHARACTER SET 'utf8'
	FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' IGNORE 1 LINES
	(lsid, @ignore, rank, scientific_name, name_lsid, @last_modified, @ignore, protologue, @ignore, is_accepted, is_draft)
	SET last_modified = case when @last_modified = '' then null else str_to_date(@last_modified, '%d-%m-%Y') end;

	
	
	
	DROP TABLE IF EXISTS taxon_name;
	
	create table taxon_name(
	lsid varchar(255),
	scientific_name varchar(700),
	title varchar(500),
	uninomial varchar(50),
	genus varchar(50),
	specific_epithet varchar(100),
	subspecific_epithet varchar(100),
	infraspecific_epithet varchar(100),
	hybrid_form varchar(500),
	authorship varchar(255),
	author_year varchar(10),
	basionym_author varchar(255),	
	rank varchar(255),
	nomen_code varchar(255),
	phrase_name char(1),
	manuscript_name char(1),	
	primary key(lsid),
	index ix_tn_name(scientific_name),
	index ix_tn_sp_ep(specific_epithet),
	index ix_tn__nomen(nomen_code),
	index ix_tn_genus(genus)
	);
	
	load data infile '/data/bie-staging/anbg/ALA_AFD_NAME.csv' 
	IGNORE INTO table taxon_name CHARACTER SET 'utf8'
	FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' IGNORE 1 LINES
	(lsid, @ignore, scientific_name, title, rank, authorship, @ignore, author_year, @ignore, @ignore, 
	nomen_code, genus, specific_epithet, subspecific_epithet, @ignore, hybrid_form, infraspecific_epithet, 
	@ignore, basionym_author, @ignore, phrase_name, manuscript_name);
	
	load data infile '/data/bie-staging/anbg/ALA_APNI_NAME.csv' 
	IGNORE INTO table taxon_name CHARACTER SET 'utf8'
	FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' IGNORE 1 LINES
	(lsid, @ignore, scientific_name, title, rank, authorship, @ignore, author_year, @ignore, @ignore, 
	nomen_code, genus, specific_epithet, subspecific_epithet, @ignore, hybrid_form, infraspecific_epithet, 
	@ignore, basionym_author, @ignore, phrase_name, manuscript_name);
	
	DROP TABLE IF EXISTS relationships;
	
	create table relationships(
	from_lsid varchar(255),
	to_lsid varchar(255),
	relationship varchar(255),
	asserted_by varchar(255),
	description varchar(255),
	index ix_rel_from(from_lsid),
	index ix_rel_to(to_lsid),
	index ix_rel_rel(relationship)
	);

 
	load data infile '/data/bie-staging/anbg/ALA_AFD_RELATIONSHIP.csv' 
	IGNORE INTO table relationships 
	FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' IGNORE 1 LINES
	(@ignore, to_lsid, from_lsid, relationship, asserted_by, description);
	
	load data infile '/data/bie-staging/anbg/ALA_APNI_RELATIONSHIP.csv' 
	IGNORE INTO table relationships 
	FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' IGNORE 1 LINES
	(@ignore, to_lsid, from_lsid, relationship, asserted_by, description);

	
	drop table if exists dictionary_relationship;
	
	create table dictionary_relationship(
		id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
		relationship varchar(255),
		description varchar(255),
		type int,
		index ix_dr_rel(relationship),
		index ix_dr_desc(description)
	);

	INSERT INTO dictionary_relationship (relationship,description,type) VALUES ('','common',7);
	INSERT INTO dictionary_relationship (relationship,description,type) VALUES ('','homonym',7);
	INSERT INTO dictionary_relationship (relationship,description,type) VALUES ('','invalid publication',7);
	INSERT INTO dictionary_relationship (relationship,description,type) VALUES ('','isonym',7);
	INSERT INTO dictionary_relationship (relationship,description,type) VALUES ('','misapplied',7);
	INSERT INTO dictionary_relationship (relationship,description,type) VALUES ('','replaced',7);
	INSERT INTO dictionary_relationship (relationship,description,type) VALUES ('','trade name',7);
	INSERT INTO dictionary_relationship (relationship,description,type) VALUES ('','variant',7);
	INSERT INTO dictionary_relationship (relationship,description,type) VALUES ('excludes','',null);
	INSERT INTO dictionary_relationship (relationship,description,type) VALUES ('has generic combination','',7);
	INSERT INTO dictionary_relationship (relationship,description,type) VALUES ('has generic combination','unplaced',7);
	INSERT INTO dictionary_relationship (relationship,description,type) VALUES ('has legislative name','',7);
	INSERT INTO dictionary_relationship (relationship,description,type) VALUES ('has miscellaneous literature name','',7);
	INSERT INTO dictionary_relationship (relationship,description,type) VALUES ('has synonym','',7);
	INSERT INTO dictionary_relationship (relationship,description,type) VALUES ('has synonym','emendation',7);
	INSERT INTO dictionary_relationship (relationship,description,type) VALUES ('has synonym','objective synonym',7);
	INSERT INTO dictionary_relationship (relationship,description,type) VALUES ('has synonym','original spelling',7);
	INSERT INTO dictionary_relationship (relationship,description,type) VALUES ('has synonym','replacement name',7);
	INSERT INTO dictionary_relationship (relationship,description,type) VALUES ('has synonym','sens. lat.',7);
	INSERT INTO dictionary_relationship (relationship,description,type) VALUES ('has synonym','subjective synonym',7);
	INSERT INTO dictionary_relationship (relationship,description,type) VALUES ('has synonym','subsequent misspelling',7);
	INSERT INTO dictionary_relationship (relationship,description,type) VALUES ('has synonym','synonym',7);
	INSERT INTO dictionary_relationship (relationship,description,type) VALUES ('has vernacular','',null);
	INSERT INTO dictionary_relationship (relationship,description,type) VALUES ('includes','incertae sedis',11);
	INSERT INTO dictionary_relationship (relationship,description,type) VALUES ('includes','nomenclatural',11);
	INSERT INTO dictionary_relationship (relationship,description,type) VALUES ('includes','species inquirenda',11);
	INSERT INTO dictionary_relationship (relationship,description,type) VALUES ('includes','taxonomic',11);
	INSERT INTO dictionary_relationship (relationship,description,type) VALUES ('includes','unplaced',11);
	INSERT INTO dictionary_relationship (relationship,description,type) VALUES ('is child taxon of','',null);
	INSERT INTO dictionary_relationship (relationship,description,type) VALUES ('is child taxon of','assertion',null);
	INSERT INTO dictionary_relationship (relationship,description,type) VALUES ('is child taxon of','classification',null);
	INSERT INTO dictionary_relationship (relationship,description,type) VALUES ('is child taxon of','unplaced',null);
	INSERT INTO dictionary_relationship (relationship,description,type) VALUES ('is congruent to','',7);
	INSERT INTO dictionary_relationship (relationship,description,type) VALUES ('is congruent to','emendation',7);
	INSERT INTO dictionary_relationship (relationship,description,type) VALUES ('is congruent to','original spelling',7);
	INSERT INTO dictionary_relationship (relationship,description,type) VALUES ('is congruent to','replacement name',7);
	INSERT INTO dictionary_relationship (relationship,description,type) VALUES ('is congruent to','subjective synonym',7);
	INSERT INTO dictionary_relationship (relationship,description,type) VALUES ('is congruent to','synonym',7);
	INSERT INTO dictionary_relationship (relationship,description,type) VALUES ('is hybrid child of','first hybrid parent',null);
	INSERT INTO dictionary_relationship (relationship,description,type) VALUES ('is hybrid child of','second hybrid parent',null);
	INSERT INTO dictionary_relationship (relationship,description,type) VALUES ('is hybrid parent of','first hybrid parent',null);
	INSERT INTO dictionary_relationship (relationship,description,type) VALUES ('is hybrid parent of','second hybrid parent',null);
	INSERT INTO dictionary_relationship (relationship,description,type) VALUES ('is parent taxon of','',null);
	INSERT INTO dictionary_relationship (relationship,description,type) VALUES ('is parent taxon of','assertion',null);
	INSERT INTO dictionary_relationship (relationship,description,type) VALUES ('is parent taxon of','classification',null);
	INSERT INTO dictionary_relationship (relationship,description,type) VALUES ('is parent taxon of','unplaced',null);
	INSERT INTO dictionary_relationship (relationship,description,type) VALUES ('overlaps','nomenclatural',null);
	INSERT INTO dictionary_relationship (relationship,description,type) VALUES ('overlaps','taxonomic',null);
	INSERT INTO dictionary_relationship (relationship,description,type) VALUES ('superseded by','',8);
	INSERT INTO dictionary_relationship (relationship,description,type) VALUES ('ambiguous synonym','COL',null);
	INSERT INTO dictionary_relationship (relationship,description,type) VALUES ('misapplied name','COL',null);
	INSERT INTO dictionary_relationship (relationship,description,type) VALUES ('synonym','COL',null);
	
	drop table if exists dictionary;
	-- Create the dictionary 
	create table dictionary(
		id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
		value varchar(255),
		type varchar(30)
	);
	
	insert into dictionary(value,type) values ('Direct Parent', 'source');
	insert into dictionary(value,type) values ('Name Group Member Parent','source');
	insert into dictionary(value,type) values ('CoL Parent', 'source');
	insert into dictionary(value,type) values ('Taxon Concept Accepted', 'accept');
	insert into dictionary(value,type) values ('Contains Parent' ,'accept');
	insert into dictionary(value,type) values ('Arbitrary', 'accept');
	insert into dictionary(value,type) values ('Synonym', 'relationship');
	insert into dictionary(value,type) values ('Reverse Synonym', 'relationship');
	insert into dictionary(value,type) values ('Parent', 'relationship');
	insert into dictionary(value,type) values ('Child', 'relationship');
	
	-- Load the relationships into the taxon_concept table
		
	-- ALA clasification generation tables
	create table ala_concepts(
		id int NOT NULL AUTO_INCREMENT primary key,
		parent_id int,
		lsid varchar(255),
		name_lsid varchar(255),
		parent_lsid varchar(255),
		parent_src int,
		src int,
		accepted_lsid varchar(255),
		rank_id int,
		lft int,
		rgt int,
		depth int,
		synonym_type int,
		genus_sound_ex varchar(255),
		sp_sound_ex varchar(255),
		insp_sound_ex varchar(255),
		col_id int,		
		source char(4),
		excluded char(1),
		
		unique index idx_ac_lsid(lsid),
		index idx_ala_name_lsid(name_lsid),
		index idx_ala_parent(parent_lsid),
		index idx_ala_accepted(accepted_lsid),
		index idx_g_sound_ex(genus_sound_ex),
		index idx_s_sound_ex(sp_sound_ex),
		index idx_i_sound_ex(insp_sound_ex),
		index idx_ac_col_id(col_id),
		index idx_ala_source(source)
	);
	
	DROP TABLE IF EXISTS ala_synonyms;
	create table ala_synonyms(
		id int NOT NULL AUTO_INCREMENT primary key,
		lsid varchar(255),
		name_lsid varchar(255),
		accepted_lsid varchar(255),
		accepted_id int,
		col_id int,
		syn_type int,
		index idx_ala_syn_name_lsid(name_lsid),
		index idx_ala_syn_ac_lsid(accepted_lsid),
		index idx_ala_syn_ac_id(accepted_id),
		index idx_ala_syn_col_id(col_id),
		index idx_ala_syn_lsid(lsid)
	);
	
	create table ala_classification(
		lsid varchar(255),
		name_lsid varchar(255),
		parent_lsid varchar(255),	
		accepted_lsid varchar(255),
		rank_id int,
		rank varchar(30),
		klsid varchar(255),
		kname varchar(255),
		plsid varchar(255),
		pname varchar(255),
		clsid varchar(255),
		cname varchar(255),
		olsid varchar(255),
		oname varchar(255),
		flsid varchar(255),
		fname varchar(255),
		glsid varchar(255),
		gname varchar(255),
		slsid varchar(255),
		sname varchar(255),
		lft int,
		rgt int,
		id int,
		parent_id int,
		excluded char(1),
		col_id int,
		primary key (lsid),
		index ix_ala_cl_rank_id(rank_id),
		index ix_ala_cl_accepted(accepted_lsid),
		index ix_ala_cl_kingdom_family(kname, fname),
		index idx_cl_name_lsid(name_lsid),
		index idx_cl_lft(lft),
		index idx_cl_rgt(rgt)
	);
	
	DROP TABLE IF EXISTS extra_identifiers;
	
	-- This table is used to store the extra identifiers to associate with an ala_concept
	-- specifically it stores the mappings when there is duplicate taxonNames in the NSL
	create table extra_identifiers(
		accepted_lsid varchar(255),
		lsid varchar(255)
	);
	
	-- taxon ranks table
	
	drop table if exists taxon_rank;
	
	create table taxon_rank(
	id int,
	rank varchar(255),
	primary key(id)
	);
	
	INSERT INTO taxon_rank values(1000,'kingdom');
	INSERT INTO taxon_rank values(1200,'subkingdom');
	INSERT INTO taxon_rank values(1800,'superphylum');
	INSERT INTO taxon_rank values(2000,'phylum');
	INSERT INTO taxon_rank values(2200,'subphylum');
	INSERT INTO taxon_rank values(2400,'cohort');
	INSERT INTO taxon_rank values(2800,'superclass');
	INSERT INTO taxon_rank values(3000,'class');
	INSERT INTO taxon_rank values(3200,'subclass');
	INSERT INTO taxon_rank values(3350,'infraclass');
	INSERT INTO taxon_rank values(3800,'superorder');
	INSERT INTO taxon_rank values(4000,'order');
	INSERT INTO taxon_rank values(4200,'suborder');
	INSERT INTO taxon_rank values(4350,'infraorder');
	INSERT INTO taxon_rank values(4400,'parvorder');
	INSERT INTO taxon_rank values(4500,'superfamily');
	INSERT INTO taxon_rank values(5000,'family');
	INSERT INTO taxon_rank values(5500,'subfamily');
	INSERT INTO taxon_rank values(5600,'tribe');
	INSERT INTO taxon_rank values(5700,'subtribe');
	INSERT INTO taxon_rank values(5999,'suprageneric');
	INSERT INTO taxon_rank values(6000,'genus');
	INSERT INTO taxon_rank values(6001,'nothogenus');
	INSERT INTO taxon_rank values(6500,'subgenus');
	INSERT INTO taxon_rank values(6600,'section');
	INSERT INTO taxon_rank values(6700,'subsection');
	INSERT INTO taxon_rank values(6800,'series');
	INSERT INTO taxon_rank values(6900,'subseries');
	INSERT INTO taxon_rank values(6950,'speciesgroup');
	INSERT INTO taxon_rank values(6975,'speciessubgroup');
	INSERT INTO taxon_rank values(7000,'species');
	INSERT INTO taxon_rank values(7001,'nothospecies');
	INSERT INTO taxon_rank values(8000,'subspecies');
	INSERT INTO taxon_rank values(8001,'nothosubspecies');
	INSERT INTO taxon_rank values(8010,'variety');
	INSERT INTO taxon_rank values(8011,'nothovariety');
	INSERT INTO taxon_rank values(8020,'form');
	INSERT INTO taxon_rank values(8021,'nothoform');
	INSERT INTO taxon_rank values(8030,'biovar');
	INSERT INTO taxon_rank values(8040,'serovar');
	INSERT INTO taxon_rank values(8050,'cultivar');
	INSERT INTO taxon_rank values(8080,'pathovar');
	INSERT INTO taxon_rank values(8090,'infraspecific');
	INSERT INTO taxon_rank values(8100,'aberration');
	INSERT INTO taxon_rank values(8110,'mutation');
	INSERT INTO taxon_rank values(8120,'race');
	INSERT INTO taxon_rank values(8130,'confersubspecies');
	INSERT INTO taxon_rank values(8140,'formaspecialis');
	INSERT INTO taxon_rank values(8150,'hybrid');
	INSERT INTO taxon_rank values(8015,'subvariety');
	INSERT INTO taxon_rank values(6925,'infrageneric');
	INSERT INTO taxon_rank values(8200,'supergenericname');
	INSERT INTO taxon_rank values(9999,'unranked');
	
		
	-- create the table used to store the extra names added from caab
	create table extra_names(
		lsid varchar(100),
		scientific_name varchar(255),
		authority varchar(200),
		common_name varchar(200),
		family varchar(100),
		genus varchar(50),
		specific_epithet varchar(100),
		primary key(lsid)
	);		
	
	-- NOW the tables for the tree files
	   DROP TABLE IF EXISTS nsl_taxon_concept;
	
	create table nsl_taxon_concept
	(
	   rank_code varchar(20),
	   rank varchar(100),
	   synonym varchar(50),
	   name varchar(200),
	   authority varchar(200),
	   full_name varchar(500),
	   name_lsid varchar(255),
	   taxon_lsid varchar(255),
	   parent_lsid varchar(255),
	   synonym_of_lsid varchar(255),
	   name_uri varchar(255),
	   taxon_uri varchar(255),
	   parent_taxon_uri varchar(255),
	   synonym_of_uri varchar(255),
	   file_source varchar(255),
	   cc_license varchar(300),
	   cc_attributionURL varchar(300),
	   excluded varchar(255),
	   source varchar(5),
	   apc_concept_lsid varchar(255),
	   synonym_same_name char(1),
	   status int,
	   child_count int,
	   synonym_count int,
	   index idx_name_tree(name_lsid),
	   index idx_taxon_tree(taxon_lsid),
	   index idx_accepted_tree(synonym_of_lsid),
	   index idx_source_tree(source),
	   index idx_tree_parent(parent_lsid)
	);
	
	load data infile '/data/bie-staging/anbg/AFD_TREE.csv'
    IGNORE INTO table nsl_taxon_concept FIELDS TERMINATED BY ',' ENCLOSED BY '\'' IGNORE 1 LINES
    SET source='AFD';
	
	load data infile '/data/bie-staging/anbg/APC_TREE.csv' 
	IGNORE INTO table nsl_taxon_concept FIELDS TERMINATED BY ',' ENCLOSED BY '\'' IGNORE 1 LINES
	SET source='APC';
	
	load data infile '/data/bie-staging/anbg/APNI_TREE.csv' 
	IGNORE INTO table nsl_taxon_concept FIELDS TERMINATED BY ',' ENCLOSED BY '\'' IGNORE 1 LINES
	SET source='APNI';

	
	-- PARENT relationship
	update taxon_concept tc, relationships r set tc.parent_lsid = r.to_lsid where r.relationship='is child taxon of' and r.from_lsid = tc.lsid;
	-- first parent of a hybrid
	
	update taxon_concept tc, relationships r set tc.parent_lsid = r.to_lsid where r.relationship='is hybrid child of' and r.description='first hybrid parent' and r.from_lsid = tc.lsid;	
	-- apply superseded flag to "accepted" taxon concepts so that they do not get included as ala concepts.
	update taxon_concept tc, relationships r set tc.is_superseded='T' where r.relationship='superseded by' and tc.lsid = r.from_lsid and tc.is_accepted='Y'
	-- apply the excluded flag to concepts that are accpeted but excluded...
	update taxon_concept tc, relationships r set tc.is_excluded='T' where r.relationship='excludes' and tc.name_lsid = r.to_lsid and tc.is_accepted='Y'
	--update the not_taxon_tree flag so that we can indicate which taxon_names are marked as accepted in the taxon csv BUT are only synonyms in the tree
	update taxon_concept tc, nsl_taxon_concept ntc set tc.no_tree_concept='T' where tc.name_lsid = ntc.name_lsid and tc.is_accepted='Y' and tc.is_superseded is null and tc.is_excluded is null and ntc.synonym_of_lsid <>''
	update taxon_concept tc, nsl_taxon_concept ntc set tc.no_tree_concept=null where tc.name_lsid = ntc.name_lsid and tc.is_accepted='Y' and tc.is_superseded is null and tc.is_excluded is null and ntc.synonym_of_lsid =''

	
	
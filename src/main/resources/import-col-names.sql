	-- CoL Stuff
	DROP TABLE IF EXISTS col_concepts;
	CREATE TABLE col_concepts
	(
	   taxon_id int NOT NULL,
	   lsid varchar(83),
	   scientific_name varchar(255),
	   rank varchar(100),
	   parent_id int,
	   kingdom_id int ,
	   kingdom_lsid varchar(83) ,
	   kingdom_name varchar(9),
	   phylum_id int ,
	   phylum_lsid varchar(83) ,
	   phylum_name varchar(21),
	   class_id int ,
	   class_lsid varchar(83),
	   class_name varchar(23),
	   order_id int ,
	   order_lsid varchar(83) ,
	   order_name varchar(24) ,
	   family_id int ,
	   family_lsid varchar(83) ,
	   family_name varchar(25) ,
	   genus_id int ,
	   genus_lsid varchar(83) ,
	   genus_name varchar(27),  
	   species_id int ,
	   species_lsid varchar(83),
	   species_name varchar(58) ,
	   infraspecies_id int ,
	   infraspecies_lsid varchar(83) ,
	   infraspecies_name varchar(37) ,   
	   author varchar(100),
	   nsl_lsid varchar(255) ,
	   lft int,
	   tgt int,
	   primary key(taxon_id),
	   index ix_col_name(scientific_name),
	   index ix_col_parent(parent_id),
	   index ix_col_lsid(lsid),
	   index ix_col_genus_lsid(genus_lsid),
	   index ix_col_genus_id(genus_id),
	   index ix_col_nsl_lsid(nsl_lsid),
	   index ix_col_fam_lsid(family_lsid),
	   index ix_col_fam_id(family_id),
	   index ix_col_kingdom(kingdom_name),
	   index ix_col_fam_name(family_name),
	   index idx_col_lft(lft),
	   index idx_col_rgt(rgt)
	);
	
	DROP TABLE IF EXISTS col_synonyms;
	create table col_synonyms(
	id int,
	scientific_name varchar(255),
	author varchar(255),
	rank varchar(100),
	kingdom varchar(100),
	accepted_id int,
	synonym_type varchar(50),
	genus varchar(100),
	specific_epithet varchar(100),
	infraspecific_epithet varchar(100),
	primary key(id),
	index ix_col_syn_accepted(accepted_id)
	);
	
	load data infile '/data/col/col_synonyms.csv'
	INTO table col_synonyms
	FIELDS TERMINATED BY '\t' (id, scientific_name, author, rank,kingdom,accepted_id,synonym_type,@ignore);
	
	load data infile '/data/col/kingdoms.csv'
	INTO table col_concepts CHARACTER SET 'utf8'
	FIELDS TERMINATED BY '\t' (kingdom_id,kingdom_lsid,kingdom_name,taxon_id,lsid,scientific_name,parent_id)
	SET rank='kingdom';
	
	load data infile '/data/col/phylum.csv'
	INTO table col_concepts CHARACTER SET 'utf8'
	FIELDS TERMINATED BY '\t' (kingdom_id,kingdom_lsid,kingdom_name,phylum_id,phylum_lsid,phylum_name,taxon_id,lsid,scientific_name,parent_id)
	SET rank='phylum';
	
	load data infile '/data/col/class.csv'
	INTO table col_concepts CHARACTER SET 'utf8'
	FIELDS TERMINATED BY '\t' (kingdom_id,kingdom_lsid,kingdom_name,phylum_id,phylum_lsid,phylum_name,class_id,class_lsid,class_name,taxon_id,lsid,scientific_name,parent_id)
	SET rank='class';
	
	load data infile '/data/col/order.csv'
	INTO table col_concepts CHARACTER SET 'utf8'
	FIELDS TERMINATED BY '\t' (kingdom_id,kingdom_lsid,kingdom_name,phylum_id,phylum_lsid,phylum_name,class_id,class_lsid,class_name,order_id,order_lsid,order_name,taxon_id,lsid,scientific_name,parent_id)
	SET rank='order';
	
	load data infile '/data/col/family.csv'
	INTO table col_concepts CHARACTER SET 'utf8'
	FIELDS TERMINATED BY '\t' (kingdom_id,kingdom_lsid,kingdom_name,phylum_id,phylum_lsid,phylum_name,class_id,class_lsid,class_name,order_id,order_lsid,order_name,family_id,family_lsid,family_name,taxon_id,lsid,scientific_name,parent_id)
	SET rank='family';
	
	load data infile '/data/col/genus.csv'
	INTO table col_concepts CHARACTER SET 'utf8'
	FIELDS TERMINATED BY '\t' (kingdom_id,kingdom_lsid,kingdom_name,phylum_id,phylum_lsid,phylum_name,class_id,class_lsid,class_name,order_id,order_lsid,order_name,family_id,family_lsid,family_name,genus_id,genus_lsid,genus_name,taxon_id,lsid,scientific_name,parent_id)
	SET rank='genus';
	
	load data infile '/data/col/species.csv'
	INTO table col_concepts CHARACTER SET 'utf8'
	FIELDS TERMINATED BY '\t' (kingdom_id,kingdom_lsid,kingdom_name,phylum_id,phylum_lsid,phylum_name,@ignore
	,class_id,class_lsid,class_name,order_id,order_lsid,order_name,family_id,family_lsid,family_name,genus_id,genus_lsid,genus_name
	,species_id,species_lsid,species_name, infraspecies_id,infraspecies_lsid,infraspecies_name,author,taxon_id,lsid,scientific_name,rank,parent_id);
	
	load data infile '/data/col/superfamilies.csv' 
	INTO table col_concepts CHARACTER SET 'utf8'
	FIELDS TERMINATED BY '\t' (taxon_id, lsid, parent_id,rank,scientific_name);
	
	-- Update the higher classification for superfamilies as this infomation was not available in the original col dump
	update col_concepts sfcc, col_concepts pcc 
	SET sfcc.kingdom_id = pcc.kingdom_id, sfcc.kingdom_name = pcc.kingdom_name, sfcc.kingdom_lsid = pcc.kingdom_lsid,
	 sfcc.phylum_id = pcc.phylum_id, sfcc.phylum_name = pcc.phylum_name, sfcc.phylum_lsid = pcc.phylum_lsid,
	 sfcc.class_id = pcc.class_id, sfcc.class_name = pcc.class_name, sfcc.class_lsid = pcc.class_lsid,
	 sfcc.order_id = pcc.order_id, sfcc.order_name = pcc.order_name, sfcc.order_lsid = pcc.order_lsid
	 where sfcc.rank = 'superfamily' and sfcc.parent_id = pcc.taxon_id;
	
	UPDATE col_concepts 
	SET author = REPLACE(author, "\n", "");
	
	UPDATE col_concepts 
	SET scientific_name = REPLACE(scientific_name, "\n", "");
	UPDATE col_concepts 
	SET author = REPLACE(author, "\r", " ");
  use ala_names; 
  -- Query OK, 1631793 rows affected (6 min 33.51 sec)
  select 'id','parent_id','lsid','parent_lsid','accepted_lsid','name_lsid','scientific_name','genus_or_higher','specific_epithet','infraspecific_epithet',
	'authorship','author_year','rank_id', 'rank','lft','rgt','kingdom_lsid','kingdom_name','phylum_lsid', 'phylum_name','class_lsid','class_name','order_lsid','order_name','family_lsid','family_name',
	'genus_lsid','genus_name','species_lsid','species_name','source','parent_src','synonym_type_id','synonym_relationship','synonym_description','raw_rank','is_excluded','col_id'
  UNION
  	select cc.taxon_id, cc.parent_id, cc.lsid,'','','',convert(cc.scientific_name using utf8), 
	case when cc.genus_name is not null then convert(cc.genus_name using utf8) when  cc.genus_name is null then convert(cc.scientific_name using utf8)else''end,
	IFNULL(cc.species_name, ''), IFNULL(cc.infraspecies_name,''), convert(IFNULL(cc.author,'') using utf8),
	'',case cc.rank when 'kingdom' then 1000 when 'phylum' then 2000 when 'class' then 3000 when 'order' then 4000 when 'family' then 5000 when 'genus' then 6000 when 'species' then 7000 when 'subspecies' then 8000 when 'form' then 8020 when 'superfamily' then 4500 when 'variety' then 8010 else 9999 end,
	cc.rank, cc.lft, cc.rgt, IFNULL(cc.kingdom_id,''), IFNULL(cc.kingdom_name, ''), IFNULL(cc.phylum_id,''), IFNULL(cc.phylum_name,''), IFNULL(cc.class_id,''), IFNULL(cc.class_name,''), IFNULL(cc.order_id,''),IFNULL(cc.order_name,''),
	IFNULL(cc.family_id,''), IFNULL(cc.family_name,''), IFNULL(cc.genus_id,''), IFNULL(cc.genus_name,''), IFNULL(cc.species_id,''), IFNULL(cc.species_name,''), 'CoL','','','','',cc.rank,'',cc.taxon_id
	INTO OUTFILE '/data/bie-staging/ala-names/col_accepted_concepts_dump.txt' FIELDS ENCLOSED BY '"'
	from col_concepts cc;
	
	-- now the synonyms
	-- Query OK, 842545 rows affected (2 min 41.99 sec)
	
	select 'id','lsid', 'name_lsid', 'accepted_lsid','accepted_id','scientific_name', 'author', 'year', 'col_id', 'syn_type', 'relatinoship', 'description'
	UNION
	select cs.id, cs.id, cs.id, cc.lsid, cs.accepted_id, cs.scientific_name, cs.author,'', cs.id, 
	case cs.synonym_type when 'synonym' then 208 when 'misapplied name' then 207 when 'ambiguous synonym' then 206 else 208 end, cs.synonym_type, 'COL'
	INTO OUTFILE '/data/bie-staging/ala-names/col_synonyms_dump.txt' FIELDS ENCLOSED BY '"'
	from col_synonyms cs join col_concepts cc on cs.accepted_id = cc.taxon_id;
	
	
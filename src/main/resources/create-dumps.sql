	
	use ala_names;
	
	update ala_concepts ac1 join ala_concepts ac2 on ac1.parent_lsid = ac2.lsid  set ac1.parent_id = ac2.id;
	
	update ala_classification cl join extra_names en on cl.glsid=en.lsid set cl.gname = en.scientific_name;
	
	update ala_classification cl join extra_names en on cl.slsid = en.lsid set cl.sname = en.scientific_name;
	
	update ala_classification cl join ala_concepts ac on cl.lsid = ac.lsid set cl.parent_id = ac.parent_id, cl.id = ac.id, cl.excluded=ac.excluded, cl.col_id = ac.col_id;
	

-- DUMP the ALA accepted concepts
-- Query OK, 224482 rows affected (7 min 17.85 sec)
-- Query OK, 224482 rows affected (26 min 29.99 sec)
    select 'id','parent_id','lsid','parent_lsid','accepted_lsid','name_lsid','scientific_name','genus_or_higher','specific_epithet','infraspecific_epithet',
	'authorship','author_year','rank_id', 'rank','lft','rgt','kingdom_lsid','kingdom_name','phylum_lsid', 'phylum_name','class_lsid','class_name','order_lsid','order_name','family_lsid','family_name',
	'genus_lsid','genus_name','species_lsid','species_name','source','parent_src','synonym_type_id','synonym_relationship','synonym_description','raw_rank','is_excluded','col_id'
	UNION
	select distinct cl.id,IFNULL(cl.parent_id,''),cl.lsid,IFNULL(cl.parent_lsid,''),IFNULL(cl.accepted_lsid,''), IFNULL(cl.name_lsid,''), 
	case when tn.scientific_name is not null and tn.scientific_name <> ''  then convert(tn.scientific_name using utf8) when tc.scientific_name is not null && tc.scientific_name<>'' then convert(tc.scientific_name using utf8)  when cc.scientific_name is not null then convert(cc.scientific_name using utf8) when en.scientific_name is not null then convert(en.scientific_name using utf8)  else ""end,
	case when tn.lsid is not null and tn.genus<>'' then convert(tn.genus using utf8)  when tn.lsid is not null and tn.genus = '' then convert(tn.scientific_name using utf8) when cc.taxon_id is not null  and cc.genus_name is not null then convert(cc.genus_name using utf8) when cc.taxon_id is not null and cc.genus_name is null then convert(cc.scientific_name using utf8) when en.genus is not null and en.genus<>"" then convert(en.genus using utf8) else "" end,
	case when tn.lsid is not null and tn.specific_epithet <>'' then convert(tn.specific_epithet using utf8) when cc.taxon_id is not null and cc.species_name then convert(cc.species_name using utf8) when en.specific_epithet is not null and en.specific_epithet<>"" then convert(en.specific_epithet using utf8) else "" end,
	case when tn.lsid is not null and tn.infraspecific_epithet<>'' then convert(tn.infraspecific_epithet using utf8) when cc.taxon_id is not null and cc.infraspecies_name is not null then convert(cc.infraspecies_name using utf8) else "" end,
	case when tn.authorship is not null and cl.lsid like 'urn:lsid:biodiversity.org.au:afd.%' then convert(tn.authorship using utf8) when tn.authorship is not null and tn.authorship <>'' and cl.lsid like 'urn:lsid:biodiversity.org.au:apni%' then convert(tn.authorship using utf8) when cc.author is not null then convert(cc.author using utf8) when en.authority is not null and en.authority<>"" then convert(en.authority using utf8) else "" end,
	case when tn.author_year is not null and cl.lsid like 'urn:lsid:biodiversity.org.au:afd.%' then "" when tn.author_year is not null and tn.author_year <>'' and cl.lsid like 'urn:lsid:biodiversity.org.au:apni%' then tn.author_year else "" end,
	IFNULL(cl.rank_id,''), IFNULL(cl.rank,''),IFNULL(cl.lft,''),IFNULL(cl.rgt,''), IFNULL(cl.klsid,''), 
	IFNULL(cl.kname,''),IFNULL(cl.plsid,''),
	IFNULL(cl.pname,''),
	IFNULL(cl.clsid,''),
	IFNULL(cl.cname,''),
	IFNULL(cl.olsid,''),IFNULL(cl.oname,''),
	IFNULL(cl.flsid,''),IFNULL(cl.fname,''),
	IFNULL(cl.glsid,''),IFNULL(convert(cl.gname using utf8),''),
	IFNULL(cl.slsid,''),IFNULL(convert(cl.sname using utf8),''), 
	case when tc.lsid like 'urn:lsid:biodiversity.org.au:afd%' then 'AFD' when tc.lsid like 'urn:lsid:biodiversity.org.au:apni%' and tc.is_accepted='Y' then 'APC' when tc.lsid like 'urn:lsid:biodiversity.org.au:apni%' and tc.is_accepted<>'Y' then 'APNI' else 'CoL'end,
	'',
	'', '', '',
	IFNULL(tc.rank, IFNULL(tn.rank, IFNULL(cc.rank,''))),IFNULL(cl.excluded,''),IFNULL(cl.col_id,'')
	INTO OUTFILE '/data/bie-staging/ala-names/ala_accepted_concepts_dump.txt' FIELDS ENCLOSED BY '"'
	from ala_classification cl 
	left join taxon_name tn on cl.name_lsid = tn.lsid 
	left join  col_concepts cc on cl.lsid = cc.lsid
	left join taxon_concept tc on cl.lsid = tc.lsid
	left join extra_names en on cl.lsid = en.lsid
	where cl.lsid is not null and cl.lsid <>''; 
	
	
-- DUMP the ALA synonyms
-- Query OK, 173871 rows affected (57.62 sec) without values in taxon name table.
-- Query OK, 173871 rows affected (2 min 16.38 sec)
-- Query OK, 183629 rows affected (4 min 31.32 sec) after adding extra synonyms

	update ala_synonyms asy, ala_concepts ac set asy.accepted_id = ac.id where asy.accepted_lsid = ac.lsid;
	

	
	select 'id','lsid', 'name_lsid', 'accepted_lsid','accepted_id','scientific_name', 'author', 'col_id', 'syn_type', 'relatinoship', 'description'
	UNION
	select alas.id + (select max(id) from ala_concepts), alas.lsid,alas.name_lsid, alas.accepted_lsid, alas.accepted_id,
	case when tn.scientific_name is not null and tn.scientific_name <> ''  then convert(tn.scientific_name using utf8)  when cols.scientific_name is not null then convert(cols.scientific_name using utf8)  else ""end, 
	case when tn.authorship is not null and alas.lsid like 'urn:lsid:biodiversity.org.au:afd.%' then convert(tn.authorship using utf8) when tn.authorship is not null and tn.authorship <>'' and alas.lsid like 'urn:lsid:biodiversity.org.au:apni%' then convert(tn.authorship using utf8) when cols.author is not null then convert(cols.author using utf8) else "" end, 
	IFNULL(alas.col_id,''), alas.syn_type, IFNULL(dr.relationship,''), IFNULL(dr.description, '')
	INTO OUTFILE '/data/bie-staging/ala-names/ala_synonyms_dump.txt' FIELDS ENCLOSED BY '"'
	FROM ala_synonyms alas
	LEFT JOIN taxon_name tn on alas.name_lsid = tn.lsid
	LEFT JOIN col_synonyms cols on alas.col_id = cols.id
	LEFT JOIN dictionary_relationship dr on alas.syn_type = dr.id;
	
	
	
-- DUMP the identifiers
-- Query OK, 435585 rows affected (4 min 54.85 sec)
	select 'name_lsid','lsid','accepted_lsid'
	UNION
	select ac.name_lsid, ac.lsid, tc.lsid
	from ala_concepts ac join taxon_concept tc on ac.name_lsid = tc.name_lsid
	where ac.lsid <> tc.lsid
	UNION
	select asyn.name_lsid, tc.name_lsid, asyn.name_lsid
	INTO OUTFILE '/data/bie-staging/ala-names/identifiers.txt' FIELDS ENCLOSED BY '"'
	from ala_synonyms asyn join taxon_concept tc on asyn.name_lsid = tc.name_lsid;
	
-- DUMP the common names
	select 'LSID', 'URI', 'Name','TaxonConcept', 'PublicationLSID','isPreferredName'
	UNION
	select r.to_lsid, '',tn.scientific_name, r.from_lsid,'',''	
	from relationships r join taxon_name tn on r.to_lsid = tn.lsid
	where relationship = 'has vernacular'
	UNION
	select lsid, '',common_name,lsid, '','Y'
	INTO outfile '/data/bie-staging/ala-names/AFD-common-names.csv'
	from extra_names
	where common_name is not null and common_name <>'';
	
	-- DUMP the potential species homonyms

	select distinct kname, pname, cname, oname, fname, gname, tn1.scientific_name,tn1.lsid, tn1.authorship, tn1.author_year,ac1.source
	INTO OUTFILE '/data/bie-staging/ala-names/ala-species-homonyms.txt'
	from ala_concepts ac1 join taxon_name tn1 on ac1.name_lsid = tn1.lsid join taxon_name tn2 on tn1.scientific_name = tn2.scientific_name
	join ala_concepts ac2 on tn2.lsid = ac2.name_lsid
	join ala_classification cl on ac1.lsid = cl.lsid
	where ac1.rank_id >=7000 and ac1.lsid <> ac2.lsid ;

	
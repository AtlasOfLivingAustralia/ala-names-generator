 	
	use ala_names_2013;
	-- 2014-01-28: Query OK, 111994 rows affected (32.34 sec)  
	update ala_concepts ac1 join ala_concepts ac2 on ac1.parent_lsid = ac2.lsid  set ac1.parent_id = ac2.id;
	
	
	update ala_concepts 
	set source = 'AFD' 
	where source is null and lsid like '%au:afd%';
	
	update ala_concepts ac join taxon_concept tc on ac.lsid = tc.lsid
	set source = 'APNI'
	where source is null and tc.lsid like '%au:apni%' and tc.is_accepted='';
	
	update ala_concepts ac join taxon_concept tc on ac.lsid = tc.lsid
	set source = 'APC'
	where source is null and tc.lsid like '%au:apni%' and tc.is_accepted='Y'
	-- 2014-01-28: Query OK, 70 rows affected (16.59 sec)
	update ala_classification cl join extra_names en on cl.glsid=en.lsid set cl.gname = en.scientific_name;
	
	-- 2104-01-28: Query OK, 139 rows affected (4.17 sec)
	update ala_classification cl join extra_names en on cl.slsid = en.lsid set cl.sname = en.scientific_name;
	
	-- 2014-01-28: Query OK, 343323 rows affected (3 min 36.75 sec)
	update ala_classification cl join ala_concepts ac on cl.lsid = ac.lsid set cl.parent_id = ac.parent_id, cl.id = ac.id, cl.excluded=ac.excluded, cl.col_id = ac.col_id, cl.source = ac.source;
	
	-- 2014-01-28: Query OK, 123294 rows affected (1 min 22.68 sec)
	update ala_classification cl, names_list nl set cl.list_id = nl.id where cl.source = nl.name;

 -- DUMP the ALA accepted concepts
 -- Query OK, 224482 rows affected (7 min 17.85 sec) 
 -- Query OK, 224482 rows affected (26 min 29.99 sec)
 -- Query OK, 338021 rows affected (11 min 17.81 sec)
 -- Query OK, 343324 rows affected (5 min 28.87 sec)
    select 'id','parent_id','lsid','parent_lsid','accepted_lsid','name_lsid','scientific_name','genus_or_higher','specific_epithet','infraspecific_epithet',
	'authorship','author_year','rank_id', 'rank','lft','rgt','kingdom_lsid','kingdom_name','phylum_lsid', 'phylum_name','class_lsid','class_name','order_lsid','order_name','family_lsid','family_name',
	'genus_lsid','genus_name','species_lsid','species_name','source','parent_src','synonym_type_id','synonym_relationship','synonym_description','raw_rank','is_excluded','col_id'
	UNION
	select cl.id,IFNULL(cl.parent_id,''),cl.lsid,IFNULL(cl.parent_lsid,''),IFNULL(cl.accepted_lsid,''), IFNULL(cl.name_lsid,''), 
    case when tn.scientific_name is not null and tn.scientific_name <> ''  then convert(tn.scientific_name using utf8) when tc.scientific_name is not null && tc.scientific_name<>'' then convert(tc.scientific_name using utf8)  when nln.scientific_name is not null then convert(nln.scientific_name using utf8) when en.scientific_name is not null then convert(en.scientific_name using utf8)  else ""end,
    case when tn.lsid is not null and tn.genus<>'' then convert(tn.genus using utf8)  when tn.lsid is not null and tn.genus = '' then convert(tn.scientific_name using utf8) when nln.genus is not null then convert(nln.genus using utf8) when nln.lsid is not null and nln.genus is null then convert(nln.scientific_name using utf8) when en.genus is not null and en.genus<>"" then convert(en.genus using utf8) else "" end,
    case when tn.lsid is not null and tn.specific_epithet <>'' then convert(tn.specific_epithet using utf8) when nln.specific_epithet is not null then convert(nln.specific_epithet using utf8) when en.specific_epithet is not null and en.specific_epithet<>"" then convert(en.specific_epithet using utf8) else "" end,
    case when tn.lsid is not null and tn.infraspecific_epithet<>'' then convert(tn.infraspecific_epithet using utf8) when nln.infraspecific_ephithet is not null then convert(nln.infraspecific_ephithet using utf8) else "" end,
    case when tn.authorship is not null and cl.lsid like 'urn:lsid:biodiversity.org.au:afd.%' then convert(tn.authorship using utf8) when tn.authorship is not null and tn.authorship <>'' and cl.lsid like 'urn:lsid:biodiversity.org.au:apni%' then convert(tn.authorship using utf8) when nln.authorship is not null then convert(nln.authorship using utf8) when en.authority is not null and en.authority<>"" then convert(en.authority using utf8) else "" end,
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
    IFNULL(cl.source, ''),
    '',
    '', '', '',
    IFNULL(tc.rank, IFNULL(tn.rank, IFNULL(nln.rank,''))),IFNULL(cl.excluded,''),IFNULL(cl.col_id,'')   
    INTO OUTFILE '/data/bie-staging/ala-names/ala_accepted_concepts_dump-2014.txt' FIELDS ENCLOSED BY '"'
    from ala_classification cl 
    left join taxon_name tn on cl.name_lsid = tn.lsid 
    left join  names_list_name nln on cl.list_id = nln.list_id and cl.lsid = nln.lsid
    left join taxon_concept tc on cl.lsid = tc.lsid
    left join extra_names en on cl.lsid = en.lsid
    where cl.lsid is not null and cl.lsid <>'';
    
	
	
-- DUMP the ALA synonyms
-- Query OK, 173871 rows affected (57.62 sec) without values in taxon name table.
-- Query OK, 173871 rows affected (2 min 16.38 sec)
-- Query OK, 183629 rows affected (4 min 31.32 sec) after adding extra synonyms
-- Query OK, 200248 rows affected (2 min 56.01 sec)

	update ala_synonyms asy, ala_concepts ac set asy.accepted_id = ac.id where asy.accepted_lsid = ac.lsid;
	

	
	select 'id','lsid', 'name_lsid', 'accepted_lsid','accepted_id','scientific_name', 'author', 'year', 'col_id', 'syn_type', 'relatinoship', 'description'
	UNION
	select alas.id + (select max(id) from ala_concepts), alas.lsid,alas.name_lsid, alas.accepted_lsid, alas.accepted_id,
    case when tn.title is not null and tn.scientific_name <> ''  then convert(tn.title using utf8)  when nln.scientific_name is not null then convert(nln.scientific_name using utf8)  else ""end, 
    case when tn.authorship is not null and alas.lsid like 'urn:lsid:biodiversity.org.au:afd.%' then convert(tn.authorship using utf8) when tn.authorship is not null and tn.authorship <>'' and alas.lsid like 'urn:lsid:biodiversity.org.au:apni%' then convert(tn.authorship using utf8) when nln.authorship is not null then convert(nln.authorship using utf8) else "" end,
    case when tn.author_year is not null then convert(tn.author_year using utf8) when nln. publication_year is not null then convert(nln.publication_year using utf8) else "" end,
    IFNULL(alas.col_id,''), alas.syn_type, IFNULL(dr.relationship,''), IFNULL(dr.description, '')
    INTO OUTFILE '/data/bie-staging/ala-names/ala_synonyms_dump-2014.txt' FIELDS ENCLOSED BY '"'
    FROM ala_synonyms alas
    LEFT JOIN taxon_name tn on alas.name_lsid = tn.lsid
    LEFT JOIN names_list_name nln on alas.list_id = nln.list_id and alas.lsid = nln.lsid
    LEFT JOIN dictionary_relationship dr on alas.syn_type = dr.id   
    group by alas.lsid, alas.accepted_lsid;
	
	
	
-- DUMP the identifiers
-- Query OK, 435585 rows affected (4 min 54.85 sec)
-- Query OK, 522522 rows affected (3 min 5.76 sec)
	select 'name_lsid','lsid','accepted_lsid'
	UNION
	select ac.name_lsid, ac.lsid, tc.lsid
	from ala_concepts ac join taxon_concept tc on ac.name_lsid = tc.name_lsid
	where ac.lsid <> tc.lsid
	UNION
	select asyn.name_lsid, asyn.lsid ,tc.lsid
	INTO OUTFILE '/data/bie-staging/ala-names/identifiers-2014.txt' FIELDS ENCLOSED BY '"'
	from ala_synonyms asyn join taxon_concept tc on asyn.name_lsid = tc.name_lsid where asyn.lsid <> tc.lsid;
	
-- DUMP the common names
-- Query OK, 23745 rows affected (7.29 sec)
	select 'LSID', 'URI', 'Name','TaxonConcept', 'PublicationLSID','isPreferredName'
	UNION
	select r.to_lsid, '',tn.scientific_name, r.from_lsid,'',''	
	from relationships r join taxon_name tn on r.to_lsid = tn.lsid
	where relationship = 'has vernacular'
	UNION
	select lsid, '',common_name,lsid, '','Y'
	INTO outfile '/data/bie-staging/ala-names/AFD-common-names-2014.csv'
	from extra_names
	where common_name is not null and common_name <>'';
	
	-- DUMP the potential species homonyms
-- Query OK, 372 rows affected (4 min 53.06 sec)
	select distinct kname, pname, cname, oname, fname, gname, tn1.scientific_name,tn1.lsid, tn1.authorship, tn1.author_year,ac1.source
	INTO OUTFILE '/data/bie-staging/ala-names/ala-species-homonyms-2014.txt'
	from ala_concepts ac1 join taxon_name tn1 on ac1.name_lsid = tn1.lsid join taxon_name tn2 on tn1.scientific_name = tn2.scientific_name
	join ala_concepts ac2 on tn2.lsid = ac2.name_lsid
	join ala_classification cl on ac1.lsid = cl.lsid
	where ac1.rank_id >=7000 and ac1.lsid <> ac2.lsid ;

	
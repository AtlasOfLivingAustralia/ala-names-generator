  use ala_names; 
  
  -- update merge_ala_concepts ac join col_concepts cc on ac.lsid = cc.lsid set col_id=cc.taxon_id; 
  
  update merge_ala_concepts set new_id = id + (select max(id) from ala_concepts);
  
  update merge_ala_concepts mc1 join merge_ala_concepts mc2 on mc1.parent_lsid = mc2.lsid set mc1.parent_id = mc2.new_id;
  
  update merge_ala_concepts mc join ala_concepts ac on mc.parent_lsid = ac.lsid set mc.parent_id = ac.id; 
  
  update merge_ala_classification cl join extra_names en on cl.glsid=en.lsid set cl.gname = en.scientific_name;
	
  update merge_ala_classification cl join extra_names en on cl.slsid = en.lsid set cl.sname = en.scientific_name;
	
  update merge_ala_classification cl join ala_concepts ac on cl.lsid = ac.lsid set cl.parent_id = ac.parent_id, cl.id = ac.id, cl.excluded=ac.excluded, cl.col_id = ac.col_id, cl.source = ac.source;
  
  update merge_ala_classification cl join merge_ala_concepts ac on cl.lsid = ac.lsid set cl.parent_id = ac.parent_id, cl.id = ac.new_id, cl.excluded=ac.excluded, cl.col_id = ac.col_id, cl.source = ac.source;

  
  -- Query OK, 1724413 rows affected, 65535 warnings (1 hour 36 min 11.45 sec)
  
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
	IFNULL(cl.source, ''),
	'',
	'', '', '',
	IFNULL(tc.rank, IFNULL(tn.rank, IFNULL(cc.rank,''))),IFNULL(cl.excluded,''),IFNULL(cl.col_id,'')
	INTO OUTFILE '/data/bie-staging/ala-names/ala_merge_accepted_concepts_dump.txt' FIELDS ENCLOSED BY '"'
	from merge_ala_classification cl 
	left join taxon_name tn on cl.name_lsid = tn.lsid 
	left join  col_concepts cc on cl.lsid = cc.lsid
	left join taxon_concept tc on cl.lsid = tc.lsid
	left join extra_names en on cl.lsid = en.lsid
	where cl.lsid is not null and cl.lsid <>'';
	
	
	-- now the synonyms
	-- insert the synonyms
	insert into merge_ala_synonyms(lsid, name_lsid, accepted_lsid, accepted_id, col_id, syn_type)  
	select concat_ws('/','species/id',ac.col_id,'synonym',cs.id), concat_ws('/','species/id',ac.col_id,'synonym',cs.id), ac.lsid,ac.new_id,cs.id,case cs.synonym_type when 'synonym' then 208 when 'misapplied name' then 207 when 'ambiguous synonym' then 206 else 208 end  
	from col_synonyms cs join merge_ala_concepts ac on cs.accepted_id = ac.col_id;
	
	-- update the new_id
	update merge_ala_synonyms set new_id = id + (select max(new_id) from merge_ala_concepts);
	
	-- Query OK, 902901 rows affected (8 min 8.77 sec)
	
	select 'id','lsid', 'name_lsid', 'accepted_lsid','accepted_id','scientific_name', 'author', 'year', 'col_id', 'syn_type', 'relatinoship', 'description'
	UNION
	select alas.id + (select max(new_id) from merge_ala_synonyms), alas.lsid,alas.name_lsid, alas.accepted_lsid, alas.accepted_id,
	case when tn.scientific_name is not null and tn.scientific_name <> ''  then convert(tn.scientific_name using utf8)  when cols.scientific_name is not null then convert(cols.scientific_name using utf8)  else ""end, 
	case when tn.authorship is not null and alas.lsid like 'urn:lsid:biodiversity.org.au:afd.%' then convert(tn.authorship using utf8) when tn.authorship is not null and tn.authorship <>'' and alas.lsid like 'urn:lsid:biodiversity.org.au:apni%' then convert(tn.authorship using utf8) when cols.author is not null then convert(cols.author using utf8) else "" end,
	case when tn.author_year is not null then convert(tn.author_year using utf8) else "" end,
	IFNULL(alas.col_id,''), alas.syn_type, IFNULL(dr.relationship,''), IFNULL(dr.description, '')	
	FROM ala_synonyms alas
	LEFT JOIN taxon_name tn on alas.name_lsid = tn.lsid
	LEFT JOIN col_synonyms cols on alas.col_id = cols.id
	LEFT JOIN dictionary_relationship dr on alas.syn_type = dr.id
	group by alas.lsid, alas.accepted_lsid
	UNION
	select syn.new_id, syn.lsid,syn.lsid,syn.accepted_lsid, syn.accepted_id, cs.scientific_name, cs.author, '', 
	syn.col_id, syn.syn_type, dr.relationship, dr.description
	INTO OUTFILE '/data/bie-staging/ala-names/ala_merge_synonyms_dump.txt' FIELDS ENCLOSED BY '"'
	from merge_ala_synonyms syn join col_synonyms cs on syn.col_id = cs.id  
	LEFT JOIN dictionary_relationship dr on syn.syn_type = dr.id;
	
	
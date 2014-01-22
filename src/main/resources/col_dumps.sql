-- COL dumps:
select kingdom_id, kingdom_lsid,kingdom_name,kingdom_id,kingdom_lsid,kingdom_name,parent_id into outfile '/data/col/kingdoms.csv' 
from _taxon_tree tt join _species_details sd on tt.taxon_id = kingdom_id where tt.rank ='kingdom' group by kingdom_id;

select kingdom_id, kingdom_lsid,kingdom_name, phylum_id,phylum_lsid,phylum_name,phylum_id,phylum_lsid, phylum_name,parent_id into outfile '/data/col/phylum.csv' 
from _taxon_tree tt join _species_details on tt.taxon_id = phylum_id where tt.rank='phylum' group by phylum_id;

select kingdom_id, kingdom_lsid,kingdom_name, phylum_id,phylum_lsid,phylum_name, class_id,class_lsid,class_name,class_id,class_lsid,class_name,parent_id into outfile '/data/col/class.csv' 
from _taxon_tree tt join _species_details on tt.taxon_id = class_id where tt.rank='class' group by class_id;

select kingdom_id, kingdom_lsid,kingdom_name, phylum_id,phylum_lsid,phylum_name,  class_id,class_lsid,class_name,order_id,order_lsid,order_name,order_id,order_lsid,order_name,parent_id into outfile '/data/col/order.csv' 
from _taxon_tree tt join _species_details on tt.taxon_id =order_id where tt.rank ='order' group by order_id;

select kingdom_id, kingdom_lsid,kingdom_name, phylum_id,phylum_lsid,phylum_name, class_id,class_lsid,class_name,order_id,order_lsid,order_name,family_id,family_lsid,family_name,family_id,family_lsid,family_name,parent_id into outfile '/data/col/family.csv' 
from _taxon_tree tt join _species_details on tt.taxon_id = family_id where tt.rank='family' group by family_id;

select kingdom_id, kingdom_lsid,kingdom_name, phylum_id,phylum_lsid,phylum_name, class_id,class_lsid,class_name,order_id,order_lsid,order_name
,family_id,family_lsid,family_name,genus_id,genus_lsid,genus_name,genus_id,genus_lsid,genus_name,parent_id into outfile '/data/col/genus.csv' 
from _taxon_tree tt join _species_details on tt.taxon_id = genus_id where tt.rank = 'genus' group by genus_id;

select kingdom_id, kingdom_lsid,kingdom_name, phylum_id,phylum_lsid,phylum_name, phylum_name, class_id,class_lsid,class_name,order_id,order_lsid,order_name
,family_id,family_lsid,family_name,genus_id,genus_lsid,genus_name,species_id,species_lsid,species_name,infraspecies_id,
infraspecies_lsid,infraspecies_name,author,tt.taxon_id,lsid,name,rank,parent_id into outfile '/data/col/species.csv' 
from _species_details sd join _taxon_tree tt on sd.taxon_id = tt.taxon_id;

-- CoL super family
select taxon_id, lsid, parent_id,rank,name into outfile '/data/col/superfamilies.csv' from _taxon_tree where rank = 'superfamily';

-- CoL synonyms
select sa.id, name, name_suffix, rank, `group`, accepted_taxon_id, sns.name_status, name_status_suffix into outfile '/data/col/col_synonyms.csv' from _search_all sa
join scientific_name_status sns on sa.name_status = sns.id where sa.name_status in (2,3,5) group by sa.name, sa.name_suffix order by sa.id;

-- CoL common names
select cne.id,cne.name,tt.lsid,tt.name 
INTO OUTFILE '/data/col/col_common_names.txt' FIELDS ENCLOSED BY '"'
from common_name_element cne 
join common_name cn on  cne.id = cn.common_name_element_id 
join _taxon_tree tt on cn.taxon_id = tt.taxon_id
where country_iso in('033', '327','241','245','239','303','188','352','227','AU','032','286','336') or (language_iso ='eng' and country_iso is null);

-- create the CSV file for the DWCA
select 'id','taxonID', 'parentNameUsageID', 'scientificName','kingdom','phylum','class',
'order','family','genus','specificEpithet', 'infraspecificEpothet','taxonRank', 'scientificNameAuthorship'
UNION
select c.taxon_id,case when c.taxon_id in (2343837,2346480,2342061,2343307,2343625,2345278,2340313,2349768,2348007,2349727)
 then concat_ws('|',c.lsid,c.taxon_id) else IFNULL(c.lsid,'') end,case when p.taxon_id in (2343837,2346480,2342061,2343307,2343625,2345278,2340313,2349768,2348007,2349727)then concat_ws('|',p.lsid,p.taxon_id) else IFNULL(p.lsid,'') end,IFNULL(c.scientific_name,''),IFNULL(c.kingdom_name,''), IFNULL(c.phylum_name,''),IFNULL(c.class_name,''),
IFNULL(c.order_name,''), IFNULL(c.family_name,''), IFNULL(c.genus_name,''), IFNULL(c.species_name,''), IFNULL(c.infraspecies_name,''), IFNULL(c.rank,''), IFNULL(c.author,'')
INTO OUTFILE '/data/bie-staging/ala-names/col_dwc.txt' FIELDS ENCLOSED BY '"'
from col_concepts c left join col_concepts p on c.parent_id = p.taxon_id


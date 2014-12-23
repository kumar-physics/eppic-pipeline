
DROP DATABASE IF EXISTS uniprot_2014_xx;
CREATE DATABASE uniprot_2014_xx;
USE uniprot_2014_xx;
DROP TABLE IF EXISTS uniprot;
DROP TABLE IF EXISTS uniprot_clusters;
CREATE TABLE uniprot (
 id varchar(23),
 uniprot_id varchar(15),
 uniparc_id char(13) PRIMARY KEY,
 tax_id int,
 sequence text
);

CREATE TABLE uniprot_clusters (
 representative varchar(15),
 member varchar(15) PRIMARY KEY,
 tax_id int
);

DROP TABLE IF EXISTS taxonomy;

CREATE TABLE taxonomy (
 tax_id int PRIMARY KEY,
 mnemonic varchar(20),
 scientific varchar(255),
 common varchar(255),
 synonym varchar(255),
 other text,
 reviewed varchar(20),
 rank varchar(20),
 lineage text,
 parent int
 
);
#LOAD DATA LOCAL INFILE '$DOWNLOAD/uniref100.tab' INTO TABLE $table;
#SHOW WARNINGS;
#LOAD DATA LOCAL INFILE '$DOWNLOAD/uniref100.clustermembers.tab' INTO TABLE ${table}_clusters;
#SHOW WARNINGS;
#LOAD DATA LOCAL INFILE '$DOWNLOAD/taxonomy-all.tab' INTO TABLE $table IGNORE 1 LINES;
#SHOW WARNINGS;
#CREATE INDEX UNIPROTID_IDX ON uniprot (uniprot_id);
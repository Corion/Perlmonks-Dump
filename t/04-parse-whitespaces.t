use strict;
use Test::More;
my @tests = do { local $/ = "###"; map { chomp; $_ } <DATA>; };

plan tests => 1+ @tests;

use_ok 'SQL::Statement::MySQL';

my $p = SQL::Parser::MySQL->new();

for my $sql (@tests) {
    chomp $sql;
    my $statement = eval { $p->parse($sql) };
    is $@, '', 'No error raised'
        or diag $sql;
};

__DATA__
--
-- Dumping data for table `document`
--
-- WHERE:  document_id between 38580000 and 38599999


-- MySQL dump 9.10
--
-- Host: localhost    Database: perlmonk_ebase
-- ------------------------------------------------------
-- Server version       4.0.17

--
-- Table structure for table `document`
--

CREATE TABLE document (
  document_id int(11) NOT NULL auto_increment,
  doctext text,
  lastedit timestamp(14) NOT NULL,
  PRIMARY KEY  (document_id)
) TYPE=MyISAM
###
--
-- Dumping data for table `document`
--
-- WHERE:  document_id between 167680000 and 167699999


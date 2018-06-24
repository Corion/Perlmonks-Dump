use strict;
use Test::More;
use Data::Dumper;
my @tests;
do {
    local $/;
    @tests = map { [ split /--/ ] } split /---/m, <DATA>;
};

plan tests => 1 + @tests * 2;

use_ok('SQL::Statement::MySQL');

for (@tests) {
    my ($sql,$expected) = @$_;
    
    my $parser = SQL::Parser::MySQL->new;
    my $statement = eval { $parser->parse($sql) };
    SKIP: {
        is $@, '', "Object was created"
            or skip "No object created", 1;
        is $statement->command, 'CREATE', 'Create statement';
        is_deeply [$statement->colvalues], [], "Correct columns parsed"
            or diag Dumper $statement->colvalues;
    };
}

__DATA__
CREATE TABLE node (
  node_id int(11) NOT NULL auto_increment,
  type_nodetype int(11) NOT NULL default '0',
  title char(240) NOT NULL default '',
  author_user int(11) NOT NULL default '0',
  createtime datetime NOT NULL default '0000-00-00 00:00:00',
  nodeupdated timestamp(14) NOT NULL,
  hits int(11) default '0',
  reputation int(11) NOT NULL default '0',
  votescast int(11) default '0',
  lockedby_user int(11) NOT NULL default '0',
  locktime datetime NOT NULL default '0000-00-00 00:00:00',
  core char(1) default '0',
  package int(11) NOT NULL default '0',
  postbonus int(1) default NULL,
  ucreatetime int(11) default NULL,
  node_iip int(11) default '0',
  PRIMARY KEY  (node_id),
  KEY title (title,type_nodetype),
  KEY author (author_user),
  KEY typecreatetime (type_nodetype,createtime),
  KEY ucreatetime (ucreatetime),
  KEY type (type_nodetype)
) TYPE=MyISAM
--
---
CREATE TABLE `picked_nodes` (
  `pick_id` int(11) NOT NULL default '0',
  `pickname` char(240) NOT NULL default '',
  `auth_id` int(11) NOT NULL default '0',
  `authname` char(240) NOT NULL default '',
  `rep` int(11) NOT NULL default '0',
  `days` int(3) NOT NULL default '0',
  `picktype` int(1) NOT NULL default '0',
  `seq` int(3) NOT NULL default '0',
  `ulastupdate` int(11) NOT NULL default '0',
  UNIQUE KEY `uidx_main` (`days`,`picktype`,`seq`),
  UNIQUE KEY `idx_pick` (`days`,`picktype`,`pick_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1 PACK_KEYS=1
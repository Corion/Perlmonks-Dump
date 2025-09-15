#!/usr/bin/perl -w
use strict;
use Getopt::Long;
use DBI;
use lib 'lib';
use SQL::Statement::MySQL;
use Data::Dumper;

GetOptions(
    'file=s'    => \my $filename,
    'outfile=s' => \my $outfilename,
    'integrity' => \my $check_only,
    'tables=s'  => \my $restrict_to_tables,
    'dsn=s'     => \my $dsn,
    'verbose'   => \my $verbose,
);

defined $filename
  or die <<EOHELP;
Syntax: $0 --file filename

Options:
  --integrity
      Only check integrity of dump file create statements
      against internal definitions.

  --file FILENAME
      Use input file FILENAME

  --tables TABLES
      Comma-separated list of tables to output
      Default is to output all tables.

  --dsn DSN
      Use given DBI DSN instead of dbi:SQLite:dbname=:memory:
      Using a non-memory DSN will reduce the RAM requirements

  --verbose
      Output more progress information
EOHELP

my $fh;
if ($filename =~ /\.bz2$/) {
    open $fh, qq(bzip2 -kdc $filename |)
        or die "Couldn't launch bzip2: $! / $?";
} else {
    open $fh, "<", $filename
        or die "Couldn't read '$filename': $!";
};

$outfilename ||= "$filename.scrubbed";
my $outfile;
if (! $check_only) {
    if ($outfilename =~ /^[|]/) {
        open $outfile, $outfilename
            or die "Couldn't spawn '$outfilename': $!";
    } else {
        open $outfile, ">", $outfilename
            or die "Couldn't create '$outfilename': $!";
    };
};

$dsn ||= 'dbi:SQLite:dbname=:memory:';
my $dbfile;
if ($dsn =~ /^dbi:SQLite:dbname=(.*)$/) {
    $dbfile = $1;
    if (-f $dbfile) {
        unlink $dbfile
            or die "Couldn't remove old file '$dbfile': $!";
    };
};

my $dbh = DBI->connect($dsn,'','',{
    RaiseError => 1, AutoCommit => 0
});
$dbh->do('PRAGMA default_synchronous=OFF');
$dbh->do('PRAGMA page_size=4096'); # For NT

sub output($) {
    print $outfile "$_[0]\n"
        unless $check_only;
}
sub progress($) { warn "$_[0]\n" }

sub do_sqlite($) {
    #warn $_[0];
    $dbh->do($_[0]);
}
sub to_sqlite {
    my ($sql) = @_;
    #warn "$sql => ";
    $sql =~ s/ (TYPE|ENGINE)=(?:MyISAM|InnoDB).*$/;/sm;
    $sql =~ s/\bauto_increment\b//gsm;
    $sql =~ s/\bint\(\d+\)/INTEGER/g;
    $sql =~ s/,\n\s*(?:UNIQUE )?KEY[^\n]*?(?=,?\n)//gs;
    $sql =~ s/ binary / /gs; # this is far from elegant
    $sql =~ s/ default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP//gs; # this is far from elegant
    #warn $sql;
    return $sql;
}

#$|++; # for debugging

# table =undef   => purge
# table =hashref => map columns:
#     column=undef   => keep
#     column=defined => force value

my %override_columns;
my ($keep,$setup_db,$clean,$postamble);
{
    local $/ = '%%';
    ($keep,$setup_db,$clean,$postamble) = map { chomp $_; $_ } <DATA>;
    close DATA;
};

sub parse_column_spec {
    my ($action,$spec) = @_;
    my (@columns) = split /,/, $spec;
    my %res;
    for my $col (@columns) {
        if ($col =~ /^\s*(\w+)(?:(?:=)(.*))?$/) {
            my ($column,$value) = ($1,$2);
            $res{$column} = $value;
        } else {
            die "Invalid column spec >>$col<<";
        }
    };
    \%res;
}

sub skip_insert { qr/^INSERT INTO \Q$_[0]\E VALUES/ };
sub skip_none   { qr/(?!)/ };

my %skip_insert;
my %columns;
my %keep_values;
my $re_skip = skip_none;
my %sql_insert;
my @dump_table;

sub output_create_statement {
    my ($sql) = @_;
    $sql =~ s/\bKEY when\b/KEY _when/g;
    output $sql;
};

sub create_table {
    my ($statement,$execute) = @_;
    my ($table) = $statement->tables;
    my $re_skip;
    my @columns = map { $_->name->{column} } $statement->columns;
    $columns{$table} = \@columns;
    my @unknown = grep {! exists $keep_values{$table}->{spec}->{$_}} @columns;
    if (@unknown) {
        warn $statement->sql;
        if ($check_only) {
            print "Unknown column(s) in $table: @unknown\n";
            $re_skip = skip_insert($table);
            $skip_insert{$table} = 1;
        } else {
            die "Unknown column(s) in $table: @unknown"
        };
    } else {
        if ($check_only) {
            $re_skip = skip_insert($table);
            $skip_insert{$table} = 1;
        } else {
            %override_columns = map {
                                    defined $keep_values{$table}->{spec}->{ $columns[$_] }
                                    ? ($_ => $keep_values{$table}->{spec}->{ $columns[$_] })
                                    : () } 0..$#columns;
            output to_sqlite( $statement->sql . ";" );

            if ($execute) {
                warn "Creating table $table\n";
                do_sqlite to_sqlite $statement->sql;
                my $sql = "INSERT INTO $table VALUES (" . join( ",", ("?") x ~~@columns) .")";
                $sql_insert{$table} = $dbh->prepare($sql);
                push @dump_table, $table;
            } else {
                warn "Outputting table $table\n";
            };
            $re_skip = skip_none;
        };
    };

    return $re_skip;
};

sub parse_keep_values {
    my ($v) = @_;
    my %keep;
    my @v = grep { /\S/ } grep { ! /^\s*#/ } map { s/\s*$//gsm; $_ } split /\n/, $v;

    for (@v) {
        if (/^\s*output\s*(\w+)\s*\((.+)\)$/) {
            my ($table,$columns) = ($1,$2);
            $keep{$table}->{spec} = parse_column_spec(\&output_row,$columns);
            $keep{$table}->{insert} = \&output_row;
            $keep{$table}->{create} = sub { create_table( $_[0], 0 ) };

        } elsif (/^\s*copy\s*(\w+)\s*\((.+)\)$/) {
            my ($table,$columns) = ($1,$2);
            $keep{$table}->{spec} = parse_column_spec(\&copy_row,$columns);
            $keep{$table}->{insert} = \&copy_row;
            $keep{$table}->{create} = sub { create_table( $_[0], 1 ) };

        } elsif (/^\s*purge\s*(\w+)$/) {
            my ($table) = ($1);
            $skip_insert{$table} = 1;
            $keep{$table}->{insert} = sub {};
            $keep{$table}->{create} = sub {
                output($_[0]->sql . ";");
                my ($table) = $_[0]->tables;
                warn "Purging $table\n";
                return skip_insert($table);
            };
        } elsif (/^\s*drop\s*(\w+)$/) {
            my ($table) = ($1);
            $skip_insert{$table} = 1;
            $keep{$table}->{insert} = sub {};
            $keep{$table}->{create} = sub {
                my ($table) = $_[0]->tables;
                warn "Removing $table\n";
                return skip_insert($table);
            };
        } else {
            die "Cannot decipher table specification from >>$_<<";
        }
    };
    return %keep;
}

%keep_values = parse_keep_values($keep);

sub copy_row {
    my ($statement) = @_;
    for my $r ($statement->colvalues) {
        my @set_values = @$r;
        @set_values[ keys %override_columns ] = values %override_columns;
        my ($table) = $statement->tables;
        $sql_insert{$table}->execute(@set_values);
    };
}

sub output_row {
    my ($statement) = @_;
    my ($table) = $statement->tables;
    for my $r ($statement->colvalues) {
        my @set_values = @$r;
        @set_values[ keys %override_columns ] = values %override_columns;
        local $" = ",";

        output "INSERT INTO $table VALUES (@set_values);";
    };
}

if (defined $restrict_to_tables) {
    my %keep = map { $_ => 1 } split /,/, $restrict_to_tables;
    my @discard = grep { ! exists $keep{$_} } keys %keep_values;
    delete @keep_values{@discard};
}

my $p = SQL::Parser::MySQL->new();
my @default_values;
my %seen_create;
my $start = time();
my $count;

# Add file "iterator" which supports:
# next_statement()
# next_create_statement()
#    by setting $/

my $override_row;
$/ = ";\n";

my @setup_db = split /;\r?\n/, $setup_db;

while (my $sql = (shift @setup_db // <$fh>)) {
    $count++;
    next if $sql =~ /$re_skip/;
    next if $sql =~ /^\s*(?:^-- [^\n]+\n)+$re_skip/m;
    next unless $sql =~ /\S/;

    my $statement = $p->parse($sql);
    #die "$sql" unless $statement;
    next unless $statement; # empty statements do happen

    if ($statement->command eq 'INSERT') {
        my ($table) = $statement->tables;
        next if $skip_insert{ $table };

        $keep_values{$table}->{insert}->($statement);

    } elsif ($statement->command eq 'CREATE') {
        $dbh->commit;
        my ($table) = $statement->tables;
        next if $seen_create{$table}++;

        # This should somehow happen in the callback anyway
        if (not exists $keep_values{$table}) {
            if ($check_only) {
                print "Ignoring/discarding table $table\n";
            } else {
                progress "Ignoring/discarding table $table (no definitions)";
                output "-- Ignoring $table";
            };
            $skip_insert{$table} = 1;
            $re_skip = skip_insert($table);
        } elsif (my $create = $keep_values{$table}->{create}) {
            $re_skip = $create->($statement);
        } else {
            die "??? $sql";
        }
    }
};
$dbh->commit;

# Now clean up the SQLite dump:
if (! $check_only) {
    progress "Cleaning database";
    for my $sql (split /;\n/, $clean) {
        $sql =~ s/^\s*--.*\n//mg;

        next unless $sql =~ /\S/;

        progress $sql if $verbose;
        my $sth = $dbh->prepare_cached($sql);
        $sth->execute();
    };

    $dbh->commit;
};

# Now, output all tables still left in the SQLite tables:
push @dump_table, 'wiki'; # special table that we create later
for my $table (@dump_table) {
    progress "Saving table '$table' from database";
    my $sql = sprintf "SELECT %s FROM %s", join( ",", @{$columns{$table}}), $table;
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    while (my $res = $sth->fetchrow_arrayref()) {
        #output "INSERT INTO $table VALUES (" . join( ",", map { "'$_'" } @$res ) . ");"

        # I think re-quoting here is not necessary, since the values are still quoted?!
        # but sometimes we get a really empty string, which was NULL before
        for (@$res) {
            if( ! defined) {
                $_ = "''"
            } elsif( $_ eq '') {
                $_ = "''";
            }
        }

        output "INSERT INTO $table VALUES (" . join( ",", @$res ) . ");"
    };
}

output "-- Postamble";
output $postamble;
output "-- End of postamble";

END {
  my $end = time();
  my $taken = (($end-$start)?($end-$start):1);
  progress sprintf "%s rows in %s seconds (%s/s)", $count, $taken, $count/$taken;
}
__DATA__
# Table definitions
output HTTP_USER_AGENT (browser,numhits)

output approval (approved_id,user_approved,whenapproved,status)
output approvalhistory (approves_id,user_approves,whenapproves,section_id,action)
output approvalstatus (approved_id,user_approved,whenapproved,status)
output approved (user_id,node_id,action,tstamp)
#output approves (approves_id,user_approves,whenapproves,action,section_id)
drop approves

drop backup_scratchpad
drop backup_user

output bug (bug_id,bugnum,assignedto_user,subsystem,status,severity,priority,summary,description,disposition)

purge cachedresults
purge cachedinfo
purge cache_stats
purge cache_store
purge chatter
purge considernodes
purge considervote

output container (container_id,context,parent_container)
output contributor (contributor_id,original_author)

purge datacache

output dailystatistics (date,numusers,lu_day,hits,lu_week,lu_2weeks,lu_4weeks,lu_ever,totalnodes)
output dbcolumn (tablename,seq,name,type,len,scale,nullable,defvalue,keyuse,extra)
output dbstatduration (durcode,durabbr)
output dbstats (stattype,duration,began,value)
output dbstattype (typecode,statabbr,statdesc,statcomment)

output devtask (devtask_id,status,priority,lead_user)

copy document (document_id,doctext,lastedit)

purge edithistory
purge editorvote

# Used for storing sent out "send me my password" details
purge emailpwd

copy generic_data_list (generic_data_list_id, add_ts, add_user_id, del_ts, del_user_id, node_id, data)

copy goodstuff (goodstuff_id, created, goodnode_id, rater_id, keywords, typeflag)

output htmlcode (htmlcode_id,code)

purge ip
purge iplog

# Are the links in use/referenced at all?
output links (from_node,to_node,linktype,hits,food)

output mail (mail_id,from_address,attachment_file)

purge message

# How does newuser relate to user??
purge newuser

output node (node_id,type_nodetype,title,author_user,createtime,nodeupdated,hits,reputation=0,votescast=0,lockedby_user=0,locktime='0000-00-00 00:00:00',core,package,postbonus=0,ucreatetime,node_iip)

output nodegroup (nodegroup_id,rank,node_id,orderby)

# force an update in all nodelets
output nodelet (nodelet_id,nltext,nlcode,updateinterval,nlgoto,parent_container,lastupdate='0')

# All nodepins lose their validity
purge nodepin
output nodehelp(nodehelp_id,nodehelp_text)
output nodetype (nodetype_id,readers_user,writers_user,deleters_user,restrict_nodetype,extends_nodetype,restrictdupes,sqltable,grouptable,updaters_user)
purge node_ipaddr

output note (note_id,parent_node,position,root_node)

output notepointers (createtime, flag, parent, child)

output patch (patch_id,for_node,field,reason,applied,applied_by)

output perlfunc (perlfunc_id,name,synopsis,description)

output perlnews (perlnews_id,linklocation)

# picked_nodes need to be re-picked
purge picked_nodes

purge pending_activations

output polls (polls_id,choices,numbers,gimmick,gimmickchoice,prologue)

# Clean out all votes on polls
purge pollvote

# Should this be dropped/purged?
purge protouser

drop rating

output reapednode (node_id,data,author_user,createtime,reason,del,keep,edit,type_nodetype,reputation=0)

# Not really needed/alive, is it?
purge referrer

output review (review_id,itemdescription,usercomment,identifier)

output scratchpad (scratchpad_id,foruser_id,privatetext='')

# Is this one used/referenced at all?
#output searchwords (keyword, hits, lastupdate, nodes)
purge searchwords

copy setting (setting_id,vars)

output snippet (snippet_id,snippetdesc,snippetcode)

output sourcecode (sourcecode_id,codedescription,codecategory,codeauthor)

# The stats aren't that interesting
purge stats

output string (string_id,text)

output testpolls (testpolls_id,choices,numbers,gimmick,gimmickchoice,prologue)
output testpollvote (ipaddress)

output themesetting (themesetting_id,parent_theme)

# The stuff in the tomb is gone for mortals
purge tomb

copy user (user_id,nick='',realname='',passwd='',passwd_hash='',email='',karma=0,givevotes='Y',votesleft=0,votes=0,user_scratchpad='',experience,imgsrc,lastupdate,lasttime,secret='',voteavg=1)

# This is just to keep the webservers in sync
#output version (version_id,version)
purge version

purge vote
purge votehistory

# It seems this table has been lost to bitrot, but we recreate it below
# It still exists on the live instance
copy wiki (wiki_id, readers, writers)

output hint (hint_id,height,width,explains_node)
output hitsinfo (hitdate,hits)
output htmlpage (htmlpage_id,pagetype_nodetype,displaytype,page,parent_container,ownedby_theme,mimetype)

# Should we purge this just to save some space?
output image (image_id,src,alt,thumbsrc,description)
# Purge this because of the email?
output newuserchit (email,digest,lasttime)
output newuserimage (newuserimage_id,timestamp)

output keyword (keyword_id,word)
output keywordnode (keyword_id,node_id,user_id)
output keywords (node_id,rating,word)
#output largedoc (largedoc_id,largedoctext)
purge largedoc
output level_buckets (experience,num,level)
output maintenance (maintenance_id,maintain_nodetype,maintaintype)

output quest (quest_id,starttime,endtime)
purge rawdata
#output rawdata (rawdata_id,datatype,databytes,lastedit)
output rawpage (rawpage_id,datatype,lastedit)
output repliesinfo (directreplies,repliesbelow,parent)
purge tickerlog

# Need some sanity check that checks that the highest _id is smaller
# or equal to SELECT max(node_id) FROM nodes

%%
-- For some reason, the table "wiki" (last table asciibetically) is missing from the backup
-- MySQL dump 10.11 , 20250907
CREATE TABLE wiki (
  wiki_id int(11) NOT NULL default 0,
  readers char(16) NOT NULL default '',
  writers char(16) NOT NULL default ''
);
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `wiki`
--

INSERT INTO wiki VALUES (73988,'lvl_3','lvl_10'),(74283,'ug_225942','ug_59438'),(76333,'lvl_1','lvl_1'),(106868,'ug_225942','ug_106850'),(96695,'ug_225942','ug_114'),(91826,'ug_225942','ug_114'),(110265,'ug_225942','ug_106850'),(133122,'ug_225942','ug_11732'),(157365,'ug_225942','ug_114'),(159568,'ug_114','ug_114'),(166549,'ug_225942','ug_114'),(168299,'ug_225942','ug_114'),(171967,'lvl_1','ug_114'),(174180,'ug_225942','ug_17342'),(178924,'ug_225942','ug_114'),(200924,'ug_225942','ug_114'),(218593,'ug_225942','ug_114'),(222493,'ug_225942','ug_106850'),(228724,'ug_225942','ug_114'),(234285,'ug_225942','ug_114'),(237008,'ug_225942','ug_17342'),(240052,'ug_225942','ug_114'),(240586,'ug_225942','ug_17342'),(242483,'ug_225942','ug_114'),(291673,'ug_225942','ug_114'),(320945,'ug_225942','ug_114'),(322009,'ug_225942','ug_225942'),(328159,'ug_225942','ug_114'),(331288,'ug_225942','ug_114'),(361810,'ug_225942','ug_114'),(368198,'ug_225942','ug_114'),(368267,'ug_114','ug_114'),(379726,'ug_225942','ug_114'),(388734,'ug_225942','ug_114'),(403440,'ug_225942','ug_114'),(403447,'ug_225942','ug_114'),(405784,'ug_225942','ug_114'),(420478,'ug_225942','ug_114'),(461491,'ug_225942','ug_106850'),(475188,'ug_225942','ug_275323'),(489631,'ug_225942','ug_275323'),(500173,'ug_225942','ug_114'),(510149,'ug_225942','ug_499790'),(517102,'ug_114','ug_114'),(535912,'ug_114','ug_114'),(545862,'lvl_1','ug_225942'),(566981,'ug_225942','ug_114'),(567891,'ug_114','ug_114'),(581860,'ug_225942','ug_114'),(591498,'ug_225942','ug_114'),(593415,'ug_225942','ug_114'),(615833,'lvl_1','ug_499790'),(617670,'ug_114','ug_114'),(617715,'ug_114','ug_114'),(686081,'ug_225942','ug_114'),(703301,'ug_114','ug_114'),(725769,'ug_225942','ug_114'),(739420,'ug_225942','ug_114'),(742222,'ug_114','ug_114'),(786306,'ug_114','ug_114'),(866617,'ug_114','ug_114'),(979477,'ug_225942','ug_114'),(1131177,'ug_114','ug_114'),(1145485,'lvl_1','ug_225942'),(1173852,'lvl_1','ug_1173852'),(1175476,'ug_1175476','ug_1175476'),(1175616,'lvl_1','ug_1175616'),(1210529,'ug_1210529','ug_1210529'),(1215407,'ug_114','ug_114'),(1217124,'lvl_10','ug_114'),(11131027,'ug_225942','ug_11732'),(11156175,'ug_106850','ug_106850');

%%
-- Purge all wiki contents especially the gods' wiki
-- also weed out the "old wiki" copies!
    UPDATE document
       SET doctext = '''*deleted*'''
     WHERE document_id IN (SELECT wiki_id FROM wiki)
;

-- Purge all user settings except mine
     UPDATE setting
        SET vars = ''
      WHERE setting_id IN (SELECT user_id FROM user where user_id not in (5348,518801))
;

%%
-- Set the magic node number for this dump
-- This requires far more thought, because we (could) need to really create a new
-- setting from scratch, which would be stupid.
-- DELETE FROM setting LEFT JOIN node ON setting_id = node_id WHERE title = 'magic_number';
-- INSERT INTO setting (setting_id,vars) --raw passthrough
--    VALUES ('magic_number', (SELECT max(node_id) FROM node));

-- Set up users that can log in
-- Corion
UPDATE user SET passwd = 's3crit' WHERE user_id = 5348;
-- Co-Rion
UPDATE user SET passwd = 's3crit' WHERE user_id = 518801;

CREATE TABLE traffic_stats (
    node_id INTEGER PRIMARY KEY NOT NULL,
    day     DATETIME,
    hits    INTEGER DEFAULT 0,
    hour    INTEGER
);

-- Create some indices:
CREATE INDEX idx_node_title on node (title);
CREATE INDEX idx_node_title_type on node (title,type_nodetype);
CREATE INDEX idx_nodegroup_node on nodegroup (node_id);
CREATE INDEX idx_nodegroup_nodegroup on nodegroup (nodegroup_id);
CREATE INDEX idx_nodegroup_node_nodegroup on nodegroup (node_id,nodegroup_id);
CREATE UNIQUE INDEX idx_htmlcode_id on htmlcode (htmlcode_id);
CREATE INDEX idx_traffic_stats ON traffic_stats (node_id,day,hour);

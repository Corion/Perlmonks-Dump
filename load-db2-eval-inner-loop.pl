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

  --dsn DSN
      Use given DBI DSN instead of dbi:SQLite:dbname=:memory:
      Using a non-memory DSN will reduce the RAM requirements

EOHELP

my $fh;
if ($filename =~ /\.bz2$/) {
    open $fh, qq(bunzip2 -kdc $filename |)
        or die "Couldn't launch bzip2: $! / $?";
} else {
    open $fh, "<", $filename
        or die "Couldn't read '$filename': $!";
};

$outfilename ||= "$filename.scrubbed";
my $outfile;
if (! $check_only) {
    warn "'$outfile'";
    if ($outfile =~ /^[|]/) {
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
sub do_sqlite($) {
    $dbh->do($_[0]);
}
sub to_sqlite {
    my ($sql) = @_;
    #warn "$sql => ";
    $sql =~ s/ TYPE=MyISAM$//sm;
    $sql =~ s/ auto_increment\b//sm;
    $sql =~ s/,\n\s*(?:UNIQUE )?KEY[^\n]*?(?=,?\n)//gs;
    $sql =~ s/ binary / /gs; # is is far from elegant
    #warn $sql;
    return $sql;
}

#$|++; # for debugging

# table =undef   => purge
# table =hashref => map columns:
#     column=undef   => keep
#     column=defined => force value

my %override_columns;
my ($keep,$clean,$postamble);
{
    local $/ = '%%';
    ($keep,$clean,$postamble) = map { chomp $_; $_ } <DATA>;
    close DATA;
};

sub parse_column_spec {
    my ($action,$spec) = @_;
    my (@columns) = split /,/, $spec;
    my %res;
    for my $col (@columns) {
        if ($col =~ /^\s*(\w+)((?:=).*)?$/) {
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

sub create_table {
    my ($statement,$execute) = @_;
    my ($table) = $statement->tables;
    my $re_skip;

    my @columns = map { $_->name } $statement->columns;
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
            output $statement->sql . ";";

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
            $keep{$table}->{insert} = q{
                my @set_values = $statement->colvalues;
                @set_values[ keys %override_columns ] = values %override_columns;
                local $" = ",";
                output "INSERT INTO $table VALUES (@set_values);";
            };

            $keep{$table}->{create} = sub { create_table( $_[0], 0 ) };

        } elsif (/^\s*copy\s*(\w+)\s*\((.+)\)$/) {
            my ($table,$columns) = ($1,$2);
            $keep{$table}->{spec} = parse_column_spec(\&copy_row,$columns);
            $keep{$table}->{insert} = q{
                my @set_values = $statement->colvalues;
                @set_values[ keys %override_columns ] = values %override_columns;
                $sql_insert{$table}->execute(@set_values);
            };
            $keep{$table}->{create} = sub { create_table( $_[0], 1 ) };

        } elsif (/^\s*purge\s*(\w+)$/) {
            my ($table) = ($1);
            $skip_insert{$table} = 1;
            $keep{$table}->{insert} = '';
            $keep{$table}->{create} = sub {
                output($_[0]->statement);
                my ($table) = $_[0]->tables;
                warn "Purging $table\n";
                return skip_insert($table);
            };
        } elsif (/^\s*drop\s*(\w+)$/) {
            my ($table) = ($1);
            $skip_insert{$table} = 1;
            $keep{$table}->{insert} = '';
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
    my ($table) = $statement->tables;
    my @set_values = $statement->colvalues;
    @set_values[ keys %override_columns ] = values %override_columns;
    $sql_insert{$table}->execute(@set_values);
}

sub output_row {
    my ($statement) = @_;
    my ($table) = $statement->tables;
    my @set_values = $statement->colvalues;
    @set_values[ keys %override_columns ] = values %override_columns;
    local $" = ",";
    output "INSERT INTO $table VALUES (@set_values);";
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

my $inner_loop = q{
    while (my $sql = <$fh>) {
        $count++;
        next if $sql =~ /$re_skip/;
        next if $sql =~ /^\s*(?:^-- [^\n]+\n)+$re_skip/m;
        next unless $sql =~ /\S/;

        my $statement = $p->parse($sql);
        if ($statement->command eq 'INSERT') {
            my ($table) = $statement->tables;
            next if $skip_insert{ $table };

            %INSERT%;

        } elsif ($statement->command eq 'CREATE') {
            $dbh->commit;
            my ($table) = $statement->tables;
            next if $seen_create{$table}++;

            # This should somehow happen in the callback anyway
            if (not exists $keep_values{$table}) {
                if ($check_only) {
                    print "Ignoring/discarding table $table\n";
                } else {
                    warn "Ignoring/discarding table $table (no definitions)\n";
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
};

# Now replace the parameters:
my %vars = (
    'INSERT' => '$keep_values{$table}->{insert}',
);

$inner_loop =~ s/%([A-Z]+)%/$vars{$1}/egsm
    or die "Didn't find anything matching >>$str<< in \n$inner_loop";
print $inner_loop;
eval $inner_loop;
die $@ if $@;

$dbh->commit;

# Now clean up the SQLite dump:
if (! $check_only) {
    for my $sql (split /;\n/, $clean) {
        $sql =~ s/^\s*--.*\n//mg;

        next if $sql =~ /^\s*$/ms;

        warn ">>$sql<<";
        my $sth = $dbh->prepare_cached($sql);
        $sth->execute();
        #$dbh->do($sql);
    };

    $dbh->commit;
};

# Now, output all tables still left in the SQLite tables:
for my $table (@dump_table) {
    my $sql = sprintf "SELECT %s FROM %s", join( ",", @{$columns{$table}}), $table;
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    warn "Writing $table from DB";
    #my $columns = join ",", map { "%s" } @{$columns{$table}};
    #my $template = "INSERT INTO $table VALUES ($columns);";
    #for (@$res) {
    while (my $res = $sth->fetchrow_arrayref()) {
        output "INSERT INTO $table VALUES (" . join( ",", @$res ) . ");"
    };
    #};
}

output "-- Postamble";
output $postamble;
output "-- End of postamble";

END {
  my $end = time();
  my $taken = (($end-$start)?($end-$start):1);
  warn sprintf "%s rows in %s seconds (%s/s)", $count, $taken, $count/$taken;
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

output htmlcode (htmlcode_id,code)

purge ip
purge iplog

# Are the links in use/referenced at all?
output links (from_node,to_node,linktype,hits,food)

output mail (mail_id,from_address,attachment_file)

purge message

# How does newuser relate to user??
output newuser (title,passwd='',realname='',email='',lastupdate)

output node (node_id,type_nodetype,title,author_user,createtime,nodeupdated,hits,reputation=0,votescast=0,lockedby_user=0,locktime='0000-00-00 00:00:00',core,package,postbonus=0,ucreatetime,node_iip)

output nodegroup (nodegroup_id,rank,node_id,orderby)

# force an update in all nodelets
output nodelet (nodelet_id,nltext,nlcode,updateinterval,nlgoto,parent_container,lastupdate='0')

# All nodepins lose their validity
purge nodepin
output nodehelp(nodehelp_id,nodehelp_text)
output nodetype (nodetype_id,readers_user,writers_user,deleters_user,restrict_nodetype,extends_nodetype,restrictdupes,sqltable,grouptable,updaters_user)

output note (note_id,parent_node,position,root_node)

output notepointers (createtime, flag, parent, child)

output patch (patch_id,for_node,field,reason,applied,applied_by)

output perlfunc (perlfunc_id,name,synopsis,description)

output perlnews (perlnews_id,linklocation)

# picked_nodes need to be re-picked
purge picked_nodes

output polls (polls_id,choices,numbers,gimmick,gimmickchoice)

# Clean out all votes on polls
purge pollvote

# Should this be dropped/purged?
drop protouser

drop rating

output reapednode (node_id,data,author_user,createtime,reason,del,keep,edit,type_nodetype,reputation=0)

# Not really needed/alive, is it?
purge referrer

output review (review_id,itemdescription,usercomment,identifier)

output scratchpad (scratchpad_id,foruser_id,privatetext='')

# Is this one used/referenced at all?
output searchwords (keyword, hits, lastupdate, nodes)

output setting (setting_id,vars)

output snippet (snippet_id,snippetdesc,snippetcode)

output sourcecode (sourcecode_id,codedescription,codecategory,codeauthor)

# The stats aren't that interesting
purge stats

output string (string_id,text)

output testpolls (testpolls_id,choices,numbers,gimmick,gimmickchoice)
output testpollvote (ipaddress)

output themesetting (themesetting_id,parent_theme)

# The stuff in the tomb is gone for mortals
purge tomb

output user (user_id,nick='',realname='',passwd='',email='',karma=0,givevotes='Y',votesleft=0,votes=0,user_scratchpad='',experience,imgsrc,lastupdate,lasttime)

output version (version_id,version)

purge vote
purge votehistory

copy wiki (wiki_id, readers, writers)

%%
-- Purge all wiki contents especially the gods' wiki
-- also weed out the "old wiki" copies!
     UPDATE document
        SET doctext = ''
      WHERE document_id IN (SELECT wiki_id FROM wiki)
;

%%
-- Set the magic node number for this dump
DELETE FROM setting WHERE setting_id = 'magic_number';
INSERT INTO setting (setting_id,values)
    VALUES ('magic_number', SELECT max(node_id) FROM nodes);

-- Set up a user that can log in
UPDATE user SET password = 's3crit' WHERE user_id = 5348;
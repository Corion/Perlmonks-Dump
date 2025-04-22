package SQL::Statement::MySQL;
use strict;
use base qw(Class::Accessor);
require SQL::Statement;
use Carp qw(croak);
use Scalar::Util qw(weaken);
use List::Util qw(max);

=head2 C<< new %ARGS >>

Creates a new instance

=cut

sub new {
    my ($class,%args) = @_;
    $class->SUPER::new(\%args);
}

__PACKAGE__->mk_accessors(qw(parser tables sql command));

sub columns { @{ $_[0]->{columns}}}

=head2 C<< where >>

Stub implementation - MySQL dumps don't contain
UPDATE, DELETE or SELECT statements.

=cut

sub where { '' };
sub colvalues {
    my ($self) = @_;

    my $values = $self->{colvalues} ||= $self->parser->parse_values($self->{_colvalues});
    my @columns = $self->columns;

    for my $v (@$values) {
        if (@$v != @columns) {
            warn ~~@$v . " / " . ~~ @columns;
            for (0.. max($#columns,$#$v)) {
                warn "$_: " . $columns[$_]->name . " => $v->[$_]"
            };
            warn "Values: @$v";
            #die "Weird statement columns: " . $self->sql;
            die;
        };
    };

    @$values
}

=head2 C<< $s->reconstruct >>

Reconstructs the SQL statement from the internal
representation. This is convenient for changing
a statement.

NOT IMPLEMENTED CURRENTLY

=cut

sub reconstruct {
    croak "reconstruct() is not implemented";
}

package SQL::Parser::MySQL;
use strict;
use base qw(Class::Accessor);
use List::Util qw( max ); # for better error reporting
use SQL::Statement;
use SQL::Statement::Util;
use vars qw(%unquote $sql_name);

$sql_name = qr/\`?(\w+)\`?/;

=head1 NAME

SQL::Parser::MySQL - parse MySQL dumps

=head1 ABSTRACT

This is a parser for SQL statements as found
in MySQL dumps.

=head1 SYNOPSIS

  use SQL::Statement::MySQL;
  my $parser = SQL::Parser::MySQL->new();
  $/ = $parser->record_separator;
  while (<>) {
      my ($stmt) = $parser->parse($_);
  }

=head2 C<< new SQL >>

Parses a single statement from a MySQL dump. While this
class attempts to behave the same as a L<SQL::Statement>
instance, it is no subclass.

=cut

__PACKAGE__->mk_accessors(qw(record_separator tables current_table));

sub new {
    my $class = shift;
    my %args;
    if (@_ == 1) {
        %args = (statement => @_)
    } else {
        %args = @_
    }
    $args{record_separator} ||= ";\n";
    $args{tables} ||= {};
    $args{current_table} = undef;

    $class->SUPER::new(\%args);
}

sub line_separator { ";\n" }

=head2 C<< parse SQL >>

Returns a L<SQL::Statement::MySQL> instance
which can be used to inspect and modify the
SQL statement. The line is autochomped.

=cut

sub parse {
    my ($self,$sql) = @_;
    my $sep = $self->line_separator;
    $sql =~ s/\Q$sep\E\z//sm;
    
    my $values;
    my ($table,$columns,@columns,$command);
    if ($sql =~ /^(?>--[^\n]*\n+)*\s*INSERT INTO\s*$sql_name\s+VALUES \((.*)\)\z/smi) {
        $command = 'INSERT';
        ($table,$values) = ($1,$2);
        if (! exists $self->tables->{$table}) {
            # who cares?
            #die "Insert into unknown table '$table' in statement >>$sql<<";
            $self->tables->{$table}->{columns} = []
        }
    } elsif ($sql =~ /^(?>--[^\n]*\n+)*\s*DROP TABLE IF EXISTS $sql_name\z/smi) {
        $command = 'DROP';
        ($table) = ($1);
        # We ignore DROP statements
        return;
    } elsif ($sql =~ /^(?>--[^\n]*\n+)*\s*LOCK TABLES $sql_name WRITE\z/smi) {
        $command = 'LOCK';
        ($table) = ($1);
        # We ignore LOCK statements
        return;
    } elsif ($sql =~ /^(?>--[^\n]*\n+)*\s*UNLOCK TABLES\z/smi) {
        $command = 'UNLOCK';
        ($table) = ($1);
        # We ignore UNLOCK statements
        return;
    } elsif ($sql =~ /^(?>--[^\n]*\n+)*\s*CREATE\s*TABLE $sql_name \((.*)\)(?: (?:TYPE|ENGINE)=(?:MyISAM|InnoDB))?(?: AUTO_INCREMENT=\d+)?(?: DEFAULT CHARSET=\S+)?(?: PACK_KEYS=1)?\s*\z/sm) {
        $command = 'CREATE';
        ($table,$columns) = ($1,$2);
        my (@columns,@keycolumns,@primaries,@unique);
        1 while ($columns =~ s/,\n\s*KEY\s*$sql_name\s*\((.*)\)\s*//);
        while ($columns =~ s/,\n\s*UNIQUE KEY\s*$sql_name\s*\((.*)\)\s*//) {
            @primaries = split /,/, $1;
        };
        if ($columns =~ s/,\n\s*PRIMARY KEY\s*\((.*)\)\s*//) {
            @primaries = split /,/, $1;
        };
        #warn $columns;
        @keycolumns = ($columns =~ s/,\n\s*KEY\s*\w+\s*\(.*\)\s*//g);
        @columns = (map { /(\w+)/ ? $1 : '<unknown>' } split /,\n/, $columns);
        $self->current_table( SQL::Statement::Table->new($table));
        $self->tables->{ $table }->{keycolumns} = \@keycolumns;
        $self->tables->{ $table }->{primarykeys} = \@primaries;
        $self->tables->{ $table }->{columns} = [ map { SQL::Statement::Util::Column->new({
                           table => $table,
                           column => $_,
                         })} @columns
                   ];

    } elsif ($sql =~ /^(?>--[^\n]*\n+)(?>\/\*[^\n]+\*\/)?\s*\z/smi) {
        # empty statement save for comments
        return
    } elsif ($sql =~ m!^(?>/\*[^\n]+\*/)?\s*\z!smi) {
        # empty statement save for comments
        return
    } else {
        die "Unknown/unparseable statement: >>$sql<<";
    }

    SQL::Statement::MySQL->new(
        sql        => $sql,
        parser     => $self,
        tables     => $table,
        columns    => [@{ $self->tables->{ $table }->{columns} }],
        _colvalues => $values,
        command    => $command,
    );
}

=head2 C<< $s->parse_values VALUES >>

Splits a comma-separated string into its components. No decoding
or unquoting of
the values is done, as the values are expected to be reused in a
subsequent C<print> statement.

The following three values are recognized:

=over 4

=item *

A literal NULL

=item *

An integer number

=item *

A single-quote quoted string. The following escapes
can occur within such a string:

  \' - a literal single quote
  \" - a literal double quote
  \\ - a backslash
  \n - a newline
  \r - a carriage return
  \0 - maybe a binary zero
  \A-\Z - something weird, maybe CTRL-A to CTRL-Z ?

=back

The function returns a reference to an array.

If you want to use the values within Perl,
for example by passing them into a DBI handle,
you can use the C<unquote_value> method to convert the
values to something directly useable by Perl.

=cut

my $string = qr/(?:(?>')(?:(?>[^\\']+)|(?>(?:\\['\\"rn0A-Z])+))*')/;
my $null   = qr/(?:(?>NULL))/;
my $num    = qr/(?:(?>-?\d+)(?:\.\d+)?)/;
my $value  = qr/(?:$string|$null|$num)/;

sub parse_values {
    my ($self,$values) = @_;
    my @r;
    
    while ($values =~ /\G(?:^|\),\(|\s*$)/gc) {
        my @res;
        while ($values =~ /\G($value)(?:,?)/gc) {
            push @res, $1;
        };
        #warn "Next: ", substr $values, pos $values, 3;
        push @r, \@res
            if @res;
    };
    
    my $consumed = pos $values;
    if (substr($values,$consumed) =~ /\S/ and $consumed != length $values) {
        while ($values =~ s/^($value),//) {
            warn "Got: $1 (and more)\n";
        };
        if ($values =~ s/^($value)//) {
            warn "Fin: $1 (nevermore)\n";
        }
        if ($values ne '') {
            warn "Left over: >$values<";
        }
        #warn $self->sql;
        #warn "Malformed VALUES clause: >$values< in $_[1]";
        #warn "Found:";
        #warn $_ for ($values =~ /((?>$value))(?>(?:,|$))/g);
        die "Cannot continue - parsing error";
    };
    return \@r;
}

=head2 C<< unquote_value VALUES >>

Takes a list of quoted values and unquotes them.
If the list contains only one element and that element is an
array reference, another reference to the mapped array
elements is returned.

The following rules apply for each value:

=item *

If the value is a literal C<NULL>, the function
returns C<undef>

=item *

If the value looks like a number, that number
is returned as a string to avoid the numeric imprecision
that's possible if conversion happens.
No explicit numerical translation is done, for example
by adding C<+0> to the number.

=item *

If the value is a string quoted within single quotes,
the following escapes get replaced
and the enclosing single quotes get stripped:

  \' - a literal single quote
  \" - a literal double quote
  \\ - a backslash
  \n - a newline
  \r - a carriage return
  \0 - a binary zero
  \A-\Z - CTRL-A to CTRL-Z ?

=cut

for my $l (qw(n r \\ 0 ' ")) {
    $unquote{$l} = eval qq{"\\$l"}
};
for my $l ('A'..'Z') {
    $unquote{$l} = chr((ord $l)-64);
}

sub unquote_value {
    my ($self) = shift;
    if (@_ == 1 and ref($_[0]) and ref($_[0]) eq 'ARRAY') {
        return [ $self->unquote_value( @$_[0]) ]
    } else {
        return map {
            if (/^$null$/) {
                undef
            } elsif (/^$num$/) {
                $_+0
            } elsif (/^$string$/) {
                # remove quotes
                chop;
                $_ = substr $_, 1;

                s/\\(.)/$unquote{$1}/eg;
                $_
            } else {
                die "Unhandled/unknown value >>$_<<"
            }
        } (@_)
    }
}

1;

=head1 SEE ALSO

L<SQL::Statement>

=cut

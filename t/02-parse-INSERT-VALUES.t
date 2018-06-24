use strict;
use Test::More tests => 1+18*2;

use_ok('SQL::Statement::MySQL');

my @tests = <DATA>;
my $long = long_escaped_string();
push @tests, "'$long'|'$long'";
push @tests, "'$long',1,2,3|'$long'|1|2|3";

$long = long_escaped_string('\\r\r\\\\');
push @tests, "'$long'|'$long'";
push @tests, "'$long',1,2,3|'$long'|1|2|3";

for (@tests) {
    chomp;
    my ($str,@data) = split /\|/, $_;
    my $visual = (length $str > 50) ? substr($str,0,46)."..." : $str;
    my $res = eval { SQL::Parser::MySQL->parse_values($str) };
    is $@, '', "No error raised for $visual";
    is_deeply($res, \@data, "Parsing $visual");
}

sub long_escaped_string {
    my ($str,$len) = @_;
    $str ||= '\r';
    $len ||= 65536;
    
    return scalar($str x $len);
};

__DATA__
1|1
NULL|NULL
'Hello World'|'Hello World'
1,1|1|1
NULL,NULL|NULL|NULL
'Hello World',''|'Hello World'|''
'a','\''|'a'|'\''
'a',1|'a'|1
'\\'|'\\'
'\'(\''|'\'(\''
'\"Unmatched \')\'\";\r\n'|'\"Unmatched \')\'\";\r\n'
'\\n\"'|'\\n\"'
'\Z\0'|'\Z\0'
-692.752917737453|-692.752917737453

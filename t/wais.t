#!/usr/bin/perl
#                              -*- Mode: Perl -*- 
# $Basename: wais.t $
# $Revision: 1.6 $
# Author          : Ulrich Pfeifer
# Created On      : Tue Dec 12 16:55:05 1995
# Last Modified By: Ulrich Pfeifer
# Last Modified On: Wed Nov 12 19:46:12 1997
# Language        : Perl
# Update Count    : 157
# Status          : Unknown, Use with caution!
#
# (C) Copyright 1997, Ulrich Pfeifer, all rights reserved.
#
#

use WAIT::Database;
use WAIT::Wais;
use Cwd;
use strict;

$SIG{__DIE__} = $SIG{INT} = \&cleanup;

my $pwd = getcwd();
print  "$^X -Iblib blib/script/bibdb -dir /tmp -database sample\n";
system "$^X -Iblib blib/script/bibdb -dir /tmp -database sample > /dev/null 2>&1";

print "1..3\n";
my $db = '/tmp/sample/bibdb';
print "# Testing WAIT searches\n";
my $result = WAIT::Wais::Search({
				 'query'    => 'pfeifer',
				 'database' => $db,
				});

&headlines($result);
my $id     = ($result->header)[9]->[6];
#$length = ($result->header)[9]->[3];
my @header = $result->header;

#my $types=($result->header)[9]->[5];
#print STDERR "\n## @$types\n";

my $short = ($result->header)[0]->[6];

my $result_text = $result->text;
print $#header >= 14 ?
    "ok 1\n" :
    "#\$\#header[$#header]result_text[$result_text]\nnot ok 1\n";

print "# Testing local retrieve\n";
$result = WAIT::Wais::Retrieve(
                              'database' => $db,
                              'docid'    => $id,
                              'query'    => 'pfeifer',
                              'type'     => 'HTML',
                             );
$result_text = $result->text;
$result_text =~ s/^/# /gm;
print $result_text =~ m!Pfeifer/Fuhr:93! ?
    "ok 2\n" :
    "# result_text[$result_text]\nnot ok 2\n";

my @x = $short->split;
print $x[2] =~ /test.ste 3585 393$/ || $x[2] == 1 ?
    "ok 3\n" :
    "# \@x:[@x]\nnot ok 3\n";


#######################################################################

sub headlines {
    my $result = shift;
    my ($tag, $score, $lines, $length, $headline, $types, $id);

    for ($result->header) {
        ($tag, $score, $lines, $length, $headline, $types, $id) = @{$_};
        printf "# %5d %5d %s %s\n",
        $score*1000, $lines, $headline, join(',', @{$types});
    }
}

sub cleanup
{
  system 'rm -rf /tmp/sample';
}


sub END
{
  &cleanup;
}

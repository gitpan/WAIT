#!/bin/sh -- # -*- perl -*- -w
eval 'exec perl -S $0 "$@"'
  if 0;

use strict;

use Config;
use File::Basename qw(fileparse);

my($file, $path) = fileparse($0);
$file =~ s!\.PL$!!i;
chdir($path)         or die "Couldn't chdir to `$path': $!\n";

print "Extracting $file\n";

open(OUT, "> $file") or die "Couldn't create `$file': $!\n";
print OUT "$Config{'startperl'} -w\n";
while (<DATA>) {
  print OUT
}
close(OUT)           or die "Couldn't close `$file': $!\n";

chmod(0755, $file)   or die "Couldn't chmod 744 on `$file': $!\n";

exec("$Config{'eunicefix'} $file") if $Config{'eunicefix'} ne ':';

__END__
######################### -*- Mode: Perl -*- #########################
##
## $Basename: smakewhatis.PL $
## $Revision: 1.6 $
##
## Author           : Ulrich Pfeifer
## Created On       : Mon Sep  2 12:57:12 1996
##
## Last Modified By : Ulrich Pfeifer
## Last Modified On : Sun Nov 22 18:44:34 1998
## 
## Copyright (c) 1996-1997, Ulrich Pfeifer
## 
## 
######################################################################

eval 'exec perl -S $0 "$@"'
  if 0;


use strict;


use FileHandle;
use File::Path;
use DB_File;
use Getopt::Long;

require WAIT::Database;
require WAIT::Config;
require WAIT::Parse::Nroff;
require WAIT::Document::Nroff;


my %OPT = (database => 'DB',
           dir      => $WAIT::Config->{WAIT_home} || '/tmp',
           table    => 'man',
           clean    => 0,
           remove   => 0,
          );

GetOptions(\%OPT,
           'database=s',
           'dir=s',
           'table=s',
           'clean!',
           'remove',
          ) || die "Usage: ...\n";

my $db;
if ($OPT{clean} and -d "$OPT{dir}/$OPT{database}") {
  eval {
    my $tmp = WAIT::Database->open(name        => $OPT{database},
                                   'directory' => $OPT{dir})
      or die "Could not open table $OPT{table}: $@";
    my $tbl = $tmp->table(name => $OPT{table});
    $tbl->drop if $tbl;
    $tmp->close;
    rmtree("$OPT{dir}/$OPT{database}/$OPT{table}",1,1)
      if -d "$OPT{dir}/$OPT{database}/$OPT{table}";
  };
  exit;
}
unless (-d "$OPT{dir}/$OPT{database}") {
  $db = WAIT::Database->create(name       => $OPT{database},
                              'directory' => $OPT{dir})
    or die "Could not open database $OPT{database}: $@";
} else {
  $db = WAIT::Database->open(name        => $OPT{database},
                             'directory' => $OPT{dir})
    or die "Could not open table $OPT{table}: $@";
}



my $layout= new WAIT::Parse::Nroff;
my $stem = [{
             'prefix'    => ['unroff', 'isotr', 'isolc'],
             'intervall' => ['unroff', 'isotr', 'isolc'],
            },'unroff', 'isotr', 'isolc', 'split2', 'stop', 'Stem'];
my $text = [{
             'prefix'    => ['unroff', 'isotr', 'isolc'],
             'intervall' => ['unroff', 'isotr', 'isolc'],
            },
             'unroff', 'isotr', 'isolc', 'split2', 'stop'];
my $sound = ['unroff', 'isotr', 'isolc', 'split2', 'Soundex'],;

my %D;

my $access = tie %D, WAIT::Document::Nroff, 'nroff -man';
die $@ unless defined $access;

my $tb = $db->table(name => $OPT{table}) ||
  $db->create_table
  (name     => $OPT{table},
   attr     => ['docid', 'headline', 'size'],
   keyset   => [['docid']],
   layout   => $layout,
   access   => $access,
   invindex =>
   [
    'name'         => $stem,
    'synopsis'     => $stem,
    'bugs'         => $stem,
    'description'  => $stem,
    'text'         => $stem,
    'environment'  => $text,
    'example'      => $text,  'example' => $stem,
    'author'       => $sound, 'author'  => $stem,
   ]
  );
die unless $tb;

my @DIRS;
if (@ARGV) {
  @DIRS = @ARGV;
} else {
  @DIRS  = @{$WAIT::Config->{manpath}};
}

my $mandir;
for $mandir (grep -d $_, @DIRS) {
  opendir(DIR, $mandir) or warn "Could not open dir '$mandir': $!";
  my @mdir =  grep -d "$mandir/$_", grep /^man/, readdir(DIR);
  closedir DIR;
  my $section;
  for $section (@mdir) {
    my $file;
    print STDERR "Scanning '$mandir/$section' ...\n";
    opendir(DIR, "$mandir/$section")
      or warn "Could not open dir '$mandir/section': $!";
    my @files =  grep -f "$mandir/$section/$_", grep $_ !~ /^\./, readdir(DIR);
    closedir DIR;
    for $file ( @files ) {
      print STDERR "Indexing '$mandir/$section/$file' ... ";
      &index("$mandir/$section/$file");
    }
  }
}
$db->close();
exit;

my $NO;
sub index {
  my $did = shift;

  if ($tb->have('docid' => $did)) {
    #die "$@" if $2 ne '';
    if (!$OPT{remove}) {
      print "duplicate\n";
      return;
    }
  } elsif ($OPT{remove}) {
    print "missing\n";
    return;
  }

  if (-s $did < 100) {
    print "too small\n";
    return;
  }

  my $value = $D{$did};
  unless (defined $value) {
    print "unavailable\n";
  }
  printf STDERR "ok [%d]\n", ++$NO;
  
  my $record = $layout->split($value);
  $record->{size} = length($value);
  my $headline = $record->{name} || $did;
  $headline =~ s/\s+/ /g; $headline =~ s/^\s+//;
  printf "%s\n", substr($headline,0,80);
  if ($OPT{remove}) {
    $tb->delete('docid' => $did, headline => $headline, %{$record});
  } else {
    $tb->insert('docid' => $did, headline => $headline, %{$record});
  }
}


__END__
## ###################################################################
## pod
## ###################################################################

=head1 NAME

smakewhatis - generate a manual database for sman

=head1 SYNOPSIS

B<smakewhatis>
[B<-database> I<database name>]
[B<-dir> I<database directory>]
[B<-table> I<name>]
[B<-remove>]
[I<mandir> ...]

=head1 DESCRIPTION

B<Smakewhatis> generates/updates databases for B<sman>(1). If
I<mandir>s are specified, these are used. Otherwise the confiigured
default directories are indexed.

=head2 OPTIONS

=over 10

=item B<-database> I<database name>

Change the default database name to I<database name>.

=item B<-dir> I<database directory>

Change the default database directory to I<database directory>.

=item B<-table> I<name>

Use I<name> instead of C<man> as table name.

=item B<-clean>

Clean B<database> before indexing.

=item B<-remove>

Remove the selected directories from the database instead of
adding/updating. This works only for the manuals which are unchanged
since the indexing.

=head1 SEE ALSO

L<sman>.

=head1 AUTHOR

Ulrich Pfeifer E<lt>F<pfeifer@ls6.informatik.uni-dortmund.de>E<gt>

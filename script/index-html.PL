#                              -*- Mode: Perl -*- 
use Config;
use File::Basename qw(&basename &dirname);

# List explicitly here the variables you want Configure to
# generate.  Metaconfig only looks for shell variables, so you
# have to mention them as if they were shell variables, not
# %Config entries.  Thus you write
#  $startperl
# to ensure Configure will look for $Config{startperl}.

# This forces PL files to create target in same directory as PL file.
# This is so that make depend always knows where to find PL derivatives.
chdir(dirname($0));
($file = basename($0)) =~ s/\.PL$//;
$file =~ s/\.pl$//
	if ($^O eq 'VMS' or $^O eq 'os2');  # "case-forgiving"

open OUT,">$file" or die "Can't create $file: $!";

print "Extracting $file (with variable substitutions)\n";

# In this section, perl variables will be expanded during extraction.
# You can use $Config{...} to use Configure variables.

print OUT <<"!GROK!THIS!";
$Config{'startperl'} -w
!GROK!THIS!
print OUT <<'!NO!SUBS!';
!NO!SUBS!

# In the following, perl variables are not expanded during extraction.

print OUT <<'!NO!SUBS!';
eval 'exec perl -w -S $0 "$@"'
    if 0;

use strict;


use FileHandle;
use Getopt::Long;

require WAIT::Database;
require WAIT::Config;
require WAIT::Parse::HTML;
require WAIT::Document::Find;


my %OPT = (database => 'DB',
           dir      => $WAIT::Config->{WAIT_home} || '/tmp',
           table    => 'kbox',
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

my $layout= new WAIT::Parse::HTML;
my $stem = [{
             'prefix'    => ['isotr', 'isolc'],
             'intervall' => ['isotr', 'isolc'],
            },'decode_entities', 'isotr', 'isolc', 'split2', 'stop', 'Stem'];
my $text = [{
             'prefix'    => ['isotr', 'isolc'],
             'intervall' => ['isotr', 'isolc'],
            },
             'decode_entities', 'isotr', 'isolc', 'split2', 'stop'];
my $sound = ['decode_entities', 'isotr', 'isolc', 'split2', 'Soundex'];

my %D;

my $access = tie (%D, 'WAIT::Document::Find',  sub { $_[0] =~ /\.htm/; }, 
		  "/usr/local/etc/httpd/htdocs/berlin");
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
    'text'         => $stem,
    'title'        => $stem,
    'title'        => $text,
   ]
  );
die unless $tb;

my @DIRS;
if (@ARGV) {
  @DIRS = @ARGV;
} else {
  @DIRS  = @{$WAIT::Config->{manpath}};
}

while (my ($path, $content) = each %D) {
  &index($path, $content);
}
$db->close();
exit;

my $NO;
sub index {
  my ($did, $value) = @_;
  if ($tb->have('docid' => $did)) {
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

  unless (defined $value) {
    print "unavailable\n";
    return;
  }
  printf STDERR "ok [%d]\n", ++$NO;
  
  my $record = $layout->split($value);
  $record->{size} = length($value);
  my $headline = $record->{title} || $did;
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
!NO!SUBS!

close OUT or die "Can't close $file: $!";
chmod 0755, $file or die "Can't reset permissions for $file: $!\n";
exec("$Config{'eunicefix'} $file") if $Config{'eunicefix'} ne ':';

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
## $Basename: sman.PL $
## $Revision: 1.6 $
##
## Author           : Ulrich Pfeifer
## Created On       : Fri Aug 30 15:52:25 1996
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


use Term::ReadLine;
use Getopt::Long;
use Fcntl;

require WAIT::Config;
require WAIT::Database;
require WAIT::Query::Base;
require WAIT::Query::Wais;


$SIG{PIPE} = 'IGNORE';
my %OPT = (database => 'DB',
           dir      => $WAIT::Config->{WAIT_home} || '/tmp',
           table    => 'man',
           pager    => $WAIT::Config->{'pager'} || 'more',
           filter   => 0,
           max      => 15,
          );

GetOptions(\%OPT,
           'database=s',
           'dir=s',
           'table=s',
           'filter=i',
           'max=i',
           'pager:s') || die "Usage: ...\n";

my $db = WAIT::Database->open(name      => $OPT{database},
                              mode      => O_RDONLY,
                              directory => $OPT{dir})
  or die "Could not open database $OPT{database}: $@";

my $tb = $db->table(name => $OPT{table})
  or die "Could not open table $OPT{table}: $@";

my $layout = $tb->layout;

my $did;
my @did;
my @stack;

my $term = new Term::ReadLine 'Simple Query Interface';

require WAIT::Format::Term;
my $format;
if ($Config::Config{'archname'} eq 'i586-linux') {
  # for color xterm
  $format = new WAIT::Format::Term query_s => "[01;31m", query_e => "[00m";
} else {
  $format = new WAIT::Format::Term;
}

my $pager =  ($OPT{pager})?\&less : \&pager;
my $OUT   = $term->OUT;

my $st = 1;
print $OUT "Enter 'h' for help.\n";

my (%hits, $query, $query_text);
while (defined ($_ = &readline("$st>"))) {
  chomp; $st++;
  if (/^$/) {
    next;
  } elsif (/^m (\d+)$/) {
    $OPT{max} = $1;
  } elsif (/^f\s*(\d+)?$/) {
    $OPT{filter} = $1;
    next;
  } elsif (/^t$/i) {
    if ($pager eq \&less) {
      $pager = \&pager;
    } else {
      $pager = \&less;
    }
    next;
  } elsif (/^(\d+)$/) {
    if (defined $did[$1]) {
      display($did[$1]);
      next;
    }
  } elsif (/^d\s*(\d+)/) {
    if (defined $did[$1]) {
      view($did[$1]);
      next;
    }
  } elsif (/^q$/i) {
    last;
  } elsif (/^l$/i) {
    # fall through
  } elsif (/^[h?]$/i) {
    help();
    next;
  } elsif (/^hh$/i) {
    extended_help();
    next;
  } else {
    $query_text = $_;
    eval {$query = WAIT::Query::Wais::query($tb, $_)};
    if ($@ ne '') {
      print $OUT "$_ => $query\n\$\@='$@'\n";
    } elsif (ref($query)) {
      %hits = $query->execute();
    } else {
      next;
    }
  }

  next unless %hits;
  my $no = 1; @did = ();
  print "Query: $query_text\n";
  for $did (sort {$hits{$b} <=> $hits{$a}} keys %hits) {
    my %tp = $tb->fetch($did);
    printf $OUT "%2d %6.3f %s\n", $no, $hits{$did},
                 substr($tp{headline} ||'',0,68);
    $did[$no] = $did;
    last if $no++ >= $OPT{max};
  }
} continue {
  # we don't do this since ANdreas Koenig does not think of it as feature
  # $term->SetHistory(grep length($_)>4, $term->GetHistory)
}

sub readline {
  if (@ARGV) {
    shift @ARGV;
  } else {
    $term->readline(@_);
  }
}
sub help {
  my $idb = "\n\t'". join(q[', '], $tb->fields()) . "'";
    print $OUT qq[Available commands:

 <num>          Show the  document <num>
 d <num>        Show the db entry of document <num>
 f <num>        Display only <num> lines context 
 h,?            Display this help message
 hh             Display query examples
 m <num>        Set maxhits to <num>
 t              Toggle display mode (term/less)
 q              Exit from $0
 l              redisplay last ranking
Other input is tried as wais query.
The following fields are known: $idb
] ;
}

sub extended_help {
  print q{
Here are some query examples:

information retrieval               free text query 
information or retrieval            same as above 
des=information retrieval           `information' must be in the description 
des=(information retrieval)         one of them in description 
des=(information or retrieval)      same as above 
des=(information and retrieval)     both of them in description 
des=(information not retrieval)     `information' in description and
                                    `retrieval' not in description 
des=(information system*)           wild-card search
au=ilia                             author names may be misspelled

You can build arbitary boolean combination of the above examples.
Filed names may be abbreviated.
}
}

sub view {
  my $did = shift;
  my %tp = $tb->fetch($did);
  for (keys %tp) {
    print $OUT "$_ $tp{$_}\n";
  }
}

sub display {
  my $did = shift;

  return unless defined $query and defined $did;
  
  print $OUT "Wais display document $did\n";
  my %tp = $tb->fetch($did);
  my $tdid = $tp{docid};
  if ($tdid !~ m(^/)) {
    $tdid = $tb->dir . '/' . $tdid;
  }
  my $buf = $tb->fetch_extern($tdid);
  if ($buf) {
    my @txt = $query->hilight($buf);
    if ($OPT{filter}) {
      @txt = &filter(@txt);
    }
    &$pager($format->as_string(\@txt));
  }
}

sub filter {
  my @result;
  my @context;
  my $lines   = 0;
  my $clines  = 0;
  my $elipsis = 0;

  print STDERR "Filter ...";
  while (@_) {
    my %tag = %{shift @_};
    my $txt =  shift @_;

    for (split /(\n)/, $txt) {
      if ($_ eq "\n") {
        if (exists $tag{_qt}) {
          #die "Weird!";
          push @result, {_i=>1}, "[WEIRD]";
        } elsif ($lines) {
          push @result, {}, $_;
          $lines--;
        } else {
          push @context, {}, $_;
          $clines++;
        }
      } else {
        if (exists $tag{_qt}) {
          push @result, {_i=>1}, "\n[ $elipsis lines ]\n" if $elipsis;
          push @result, @context, {%tag}, $_;
          delete $tag{_qt};
          @context = (); $clines = 0; $elipsis=0;
          $lines = $OPT{filter}+1;
        } elsif ($lines) {
          push @result, \%tag, $_;
        } else {
          push @context, \%tag, $_;
        }
      }
      if ($clines>$OPT{filter}) {
        my (%tag, $txt);
        while ($clines>$OPT{filter}) {
          %tag = %{shift @context};
          $txt =  shift @context;
          if ($txt =~ /\n/) {
            $clines--;
            $elipsis++;
          }
        }
      }
    }
  }
  print STDERR " done\n";
  @result;
}

sub less {
  my $flags;
  if ($WAIT::Config->{pager} =~ /less/) {
    $flags = '-r';
  } elsif ($WAIT::Config->{pager} =~ /more/) {
    $flags = '-c';
  }
  open(PAGER, "|$WAIT::Config->{pager} $flags") or die;
  print PAGER @_;
  close PAGER;
}

sub pager {
  my @lines = split /\n/, $_[0];
  my $line = 0;
  for (@lines) {
    print "$_\n"; $line++;
    if ($line % 24 == 0) {
      my $key = readline("[return]");
      return if $key =~ /^q/i;
    }
  }
}


__END__
## ###################################################################
## pod
## ###################################################################

=head1 NAME

sman - Search and disply manuals interactive

=head1 SYNOPSIS

B<sman>
[B<-database> I<database name>]
[B<-dir> I<database directory>]
[B<-table> I<name>]
[B<-less>]
[B<-filter> I<num>]
[B<-max> I<num>]

=head1 DESCRIPTION

B<Sman> is an interactive search interface to your systems manual pages.

=head2 OPTIONS

=over 10

=item B<-database> I<database name>

Change the default database name to I<database name>.

=item B<-dir> I<database directory>

Change the default database directory to I<database directory>.

=item B<-table> I<name>

Use I<name> instead of C<man> as table name.

=item B<-pager> I<name>

Use I<name> instead of the default pager. If no I<name> is supplied a
buildin pager is used.

=item B<-filter> I<num>

Display only I<num> lines above and below an occurance of a search
term in the manual.

=item B<-max> I<num>

Display only I<num>  hits. Default is to 10.

=head1 SEE ALSO

L<smakewhatis>.

=head1 AUTHOR

Ulrich Pfeifer E<lt>F<pfeifer@ls6.informatik.uni-dortmund.de>E<gt>

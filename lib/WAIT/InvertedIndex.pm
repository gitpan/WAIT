#                              -*- Mode: Cperl -*- 
# InvertedIndex.pm --
# ITIID           : $ITI$ $Header $__Header$
# Author          : Ulrich Pfeifer
# Created On      : Thu Aug  8 13:05:10 1996
# Last Modified By: Ulrich Pfeifer
# Last Modified On: Sun Nov 22 18:44:42 1998
# Language        : CPerl
# Status          : Unknown, Use with caution!
#
# Copyright (c) 1996-1997, Ulrich Pfeifer
#

package WAIT::InvertedIndex;
use strict;
use DB_File;
use Fcntl;
use WAIT::Filter;
use Carp;
use vars qw(%FUNC);

my $O = pack('C', 0xff)."o";                  # occurances
my $M = pack('C', 0xff)."m";                  # maxtf

sub new {
  my $type = shift;
  my %parm = @_;
  my $self = {};

  $self->{file}     = $parm{file}     or croak "No file specified";
  $self->{attr}     = $parm{attr}     or croak "No attributes specified";
  $self->{filter}   = $parm{filter};
  $self->{'name'}   = $parm{'name'};
  $self->{records}  = 0;
  for (qw(intervall prefix)) {
    if (exists $parm{$_}) {
      if (ref $parm{$_}) {
        $self->{$_} = [@{$parm{$_}}] # clone
      } else {
        $self->{$_} = $parm{$_}
      }
    }
  }
  bless $self, ref($type) || $type;
}

sub name {$_[0]->{'name'}}

sub _split_pos {
  my ($text, $pos) = @{$_[0]};
  my @result;

  $text =~ s/(^\s+)// and $pos += length($1);
  while ($text =~ s/(^\S+)//) {
    my $word = $1;
    push @result, [$word, $pos];
    $pos += length($word);
    $text =~ s/(^\s+)// and $pos += length($1);
  }
  @result;
}

sub _xfiltergen {
  my $filter = pop @_;

# Oops, we cannot overrule the user's choice. Other filters may kill
# stopwords, such as isotr clobbers "isn't" to "isnt".

#  if ($filter eq 'stop') {      # avoid the slow stopword elimination
#    return _xfiltergen(@_);            # it's cheaper to look them up afterwards
#  }
  if (@_) {
    if ($filter =~ /^split(\d*)/) {
      if ($1) {
        "grep(length(\$_->[0])>=$1, map(&WAIT::Filter::split_pos(\$_), " . _xfiltergen(@_) .'))' ;
      } else {
        "map(&WAIT::Filter::split_pos(\$_), " . _xfiltergen(@_) .')' ;
      }
    } else {
      "map ([&WAIT::Filter::$filter(\$_->[0]), \$_->[1]]," ._xfiltergen(@_) .')';
    }
  } else {
    if ($filter =~ /^split(\d*)/) {
      if ($1) {
        "grep(length(\$_->[0])>=$1, map(&WAIT::Filter::split_pos(\$_), [\$_[0], 0]))" ;
      } else {
        "map(&WAIT::Filter::split_pos(\$_), [\$_[0], 0])" ;
      }
    } else {
      "map ([&WAIT::Filter::$filter(\$_->[0]), \$_->[1]], [\$_[0], 0])";
    }
  }
}

sub parse_pos {
  my $self = shift;

  unless (exists $self->{xfunc}) {
    $self->{xfunc}     =
      eval sprintf("sub {%s}", _xfiltergen(@{$self->{filter}}));
    #printf "\nsub{%s}$@\n", _xfiltergen(@{$self->{filter}});
  }
  &{$self->{xfunc}}($_[0]);
}

sub _filtergen {
  my $filter = pop @_;

  if (@_) {
    "map(&WAIT::Filter::$filter(\$_), " . _filtergen(@_) . ')';
  } else {
    "map(&WAIT::Filter::$filter(\$_), \@_)";
  }
}

sub drop {
  my $self = shift;
  if ((caller)[0] eq 'WAIT::Table') { # Table knows about this
    my $file = $self->{file};

    ! (!-e $file or unlink $file);
  } else {                              # notify our database
    croak ref($self)."::drop called directly";
  }
}

sub open {
  my $self = shift;
  my $file = $self->{file};

  if (defined $self->{dbh}) {
    $self->{dbh};
  } else {
    $self->{func}     =
      eval sprintf("sub {grep /./, %s}", _filtergen(@{$self->{filter}}));
    $self->{dbh} = tie(%{$self->{db}}, 'DB_File', $file,
                       $self->{mode}, 0664, $DB_BTREE);
#    tie(%{$self->{cache}}, 'DB_File', undef,
#        $self->{mode}, 0664, $DB_BTREE)
    $self->{cache} = {}
      if $self->{mode} & O_RDWR;
#    tie(%{$self->{cdict}}, 'DB_File', undef,
#        $self->{mode}, 0664, $DB_BTREE)
    $self->{cdict} = {}
      if $self->{mode} & O_RDWR;
    $self->{cached} = 0;
  }
}

sub insert {
  my $self  = shift;
  my $key   = shift;
  my %occ;

  defined $self->{db} or $self->open;
  grep $occ{$_}++, &{$self->{func}}(@_);
  my ($word, $noc);
  $self->{records}++;
  while (($word, $noc) = each %occ) {
    if (defined $self->{cache}->{$word}) {
      $self->{cdict}->{$O,$word}++;
      $self->{cache}->{$word} .= pack 'w2', $key, $noc;
    } else {
      $self->{cdict}->{$O,$word} = 1;
      $self->{cache}->{$word}  = pack 'w2', $key, $noc;
    }
    $self->{cached}++;
  }
  $self->sync if $self->{cached} > 100_000;
  my $maxtf = 0;
  for (values %occ) {
    $maxtf = $_ if $_ > $maxtf;
  }
  $self->{db}->{$M, $key} = $maxtf;
}

sub delete {
  my $self  = shift;
  my $key   = shift;
  my %occ;

  defined $self->{db} or $self->open;
  $self->sync;
  $self->{records}--;
  grep $occ{$_}++, &{$self->{func}}(@_);
  for (keys %occ) {
    # may reorder posting list
    my %post = unpack 'w*', $self->{db}->{$_};
    $self->{db}->{$O,$_}--;
    delete $post{$key};
    $self->{db}->{$_} = pack 'w*', %post;
  }
  delete $self->{db}->{$M, $key};
}

sub intervall {
  my ($self, $first, $last) = @_;
  my $value = '';
  my $word  = '';
  my @result;

  return unless exists $self->{'intervall'};

  defined $self->{db} or $self->open;
  $self->sync;
  my $dbh = $self->{dbh};       # for convenience

  if (ref $self->{'intervall'}) {
    unless (exists $self->{'ifunc'}) {
      $self->{'ifunc'} =
        eval sprintf("sub {grep /./, %s}", _filtergen(@{$self->{intervall}}));
    }
    ($first) = &{$self->{'ifunc'}}($first) if $first;
    ($last)  = &{$self->{'ifunc'}}($last) if $last;
  }
  if (defined $first and $first ne '') {         # set the cursor to $first
    $dbh->seq($first, $value, R_CURSOR);
  } else {
    $dbh->seq($first, $value, R_FIRST);
  }
  # We assume that word do not start with the character \377
  # $last = pack 'C', 0xff unless defined $last and $last ne '';
  return () if defined $last and $first gt $last; # $first would be after the last word
  
  push @result, $first;
  while (!$dbh->seq($word, $value, R_NEXT)) {
    # We should limit this to a "resonable" number of words
    last if (defined $last and $word gt $last) or $word =~ /^($M|$O)/o;
    push @result, $word;
  }
  \@result;                     # speed
}

sub prefix {
  my ($self, $prefix) = @_;
  my $value = '';
  my $word  = '';
  my @result;

  return () unless defined $prefix; # Full dictionary requested !!
  return unless exists $self->{'prefix'};
  defined $self->{db} or $self->open;
  $self->sync;
  my $dbh = $self->{dbh};
  
  if (ref $self->{'prefix'}) {
    unless (exists $self->{'pfunc'}) {
      $self->{'pfunc'} =
        eval sprintf("sub {grep /./, %s}", _filtergen(@{$self->{prefix}}));
    }
    ($prefix) = &{$self->{'pfunc'}}($prefix);
  }

  if ($dbh->seq($word = $prefix, $value, R_CURSOR)) {
    return ();
  }
  return () if $word !~ /^$prefix/;
  push @result, $word;

  while (!$dbh->seq($word, $value, R_NEXT)) {
    # We should limit this to a "resonable" number of words
    last if $word !~ /^$prefix/;
    push @result, $word;
  }
  \@result;                     # speed
}

sub search {
  my $self  = shift;

  defined $self->{db} or $self->open;
  $self->sync;
  $self->search_raw(&{$self->{func}}(@_)); # No call to parse() here
}

sub parse {
  my $self  = shift;

  defined $self->{db} or $self->open;
  &{$self->{func}}(@_);
}

sub keys {
  my $self  = shift;

  defined $self->{db} or $self->open;
  keys %{$self->{db}};
}

sub search_prefix {
  my $self  = shift;

  # print "search_prefix(@_)\n";
  defined $self->{db} or $self->open;
  $self->search_raw(map($self->prefix($_), @_));
}

sub search_raw {
  my $self  = shift;
  my %occ;
  my %score;

  return () unless @_;

  defined $self->{db} or $self->open;
  $self->sync;
  grep $occ{$_}++, @_;
  for (keys %occ) {
    if (defined $self->{db}->{$_}) {
      my %post = unpack 'w*', $self->{db}->{$_};
      my $idf = log($self->{records}/($self->{db}->{$O,$_} || 1));
      my $did;
      for $did (keys %post) {
        $score{$did} = 0 unless defined $score{$did}; # perl -w 
        $score{$did} += $post{$did} / $self->{db}->{$M, $did} * $idf
          if $self->{db}->{$M, $did}; # db may be broken
      }
    }
  }
  %score;
}

sub sync {
  my $self = shift;

  if ($self->{mode} & O_RDWR) {
    print STDERR "Flushing $self->{cached} postings\n";
    while (my($key, $value) = each %{$self->{cache}}) {
      $self->{db}->{$key} .= $value;
      #delete $self->{cache}->{$key};
    }
    while (my($key, $value) = each %{$self->{cdict}}) {
      $self->{db}->{$key} = 0 unless  $self->{db}->{$key};
      $self->{db}->{$key} += $value;
      #delete $self->{cdict}->{$key};
    }
    $self->{cache} = {};
    $self->{cdict} = {};
    # print STDERR "*** $self->{cache} ", tied(%{$self->{cache}}), "==\n";
    $self->{cached} = 0;
    # $self->{dbh}->sync if $self->{dbh};
  }
}

sub close {
  my $self = shift;

  if ($self->{dbh}) {
    $self->sync;
    delete $self->{dbh};
    untie %{$self->{db}};
    delete $self->{db};
    delete $self->{func};
    delete $self->{cache};
    delete $self->{cached};
    delete $self->{cdict};
    delete $self->{pfunc} if defined $self->{pfunc};
    delete $self->{ifunc} if defined $self->{ifunc};
    delete $self->{xfunc} if defined $self->{xfunc};
  }
}

1;


#                              -*- Mode: Perl -*- 
# Table.pm -- 
# ITIID           : $ITI$ $Header $__Header$
# Author          : Ulrich Pfeifer
# Created On      : Thu Aug  8 13:05:10 1996
# Last Modified By: Ulrich Pfeifer
# Last Modified On: Sun Nov 22 18:44:37 1998
# Language        : CPerl
# Update Count    : 51
# Status          : Unknown, Use with caution!
# 
# Copyright (c) 1996-1997, Ulrich Pfeifer
# 

=head1 NAME

WAIT::Table -- Module for maintaining Tables / Relations

=head1 SYNOPSIS

  require WAIT::Table;

=head1 DESCRIPTION

=cut

package WAIT::Table;
require WAIT::Parse::Base;
use strict;
use Carp;
use DB_File;
use Fcntl;

my $USE_RECNO = 0;

=head2 Creating a Table.

The constructor WAIT::Table-<gt>new is normally called via the
create_table method of a database handle. This is not enforced, but
creating a table doesn not make any sense unless the table is
registered by the database because the latter implements persistence
of the meta data. Registering is done automatically by letting the
database handle create a table.

  my $db = create WAIT::Database name => 'sample';
  my $tb = $db->create_table (name     => 'test',
                              attr     => ['docid', 'headline'],
                              layout   => $layout,
                              access   => $access,
                             );

The constructor returns a handle for the table. This handle is hidden by the
table module, to prevent direct access if called via Table.

=over 10

=item C<access> => I<accesobj>

A reference to a acces object for the external parts (attributes) of
tuples. As you may remember, the WAIT System does not enforce that
objects are completely stored inside the system to avoid duplication.
There is no (strong) point in storing all you HTML-Documents inside
the system when indexing your WWW-Server.

=item C<file> => I<fname>

The filename of the records file. Files for indexes will have I<fname>
as prefix. I<Mandatory>

=item C<name> => I<name>

The name of this table. I<Mandatory>

=item C<attr> => [ I<attr> ... ]

A reference to an array of attribute names. I<Mandatory>

=item C<djk> => [ I<attr> ... ]

A reference to an array of attribute names which make up the
I<disjointness key>. Don't think about it - i's of no use yet;

=item C<layout> => I<layoutobj>

A reference to an external parser object. Defaults to anew instance of
C<WAIT::Parse::Base>

=item C<access> => I<accesobj>

A reference to a acces object for the external parts of tuples.

=back

=cut

sub new {
  my $type = shift;
  my %parm = @_;
  my $self = {};

  # Do that before we eventually add '_weight' to attributes.
  $self->{keyset}   = $parm{keyset}   || [[@{$parm{attr}}]];
  $self->{mode}     = O_CREAT | O_RDWR;
  # Determine and set up subclass
  $type = ref($type) || $type;
  if (defined $parm{djk}) {
    if (@{$parm{djk}} == @{$parm{attr}}) {
      # All attributes in DK (sloppy test here!)
      $type .= '::Independent';
      require WAIT::Table::Independent;
    } else {
      $type .= '::Disjoint';
      require WAIT::Table::Disjoint;
    }
    # Add '_weight' to attributes
    my %attr;
    @attr{@{$parm{attr}}} = (1) x @{$parm{attr}};
    unshift @{$parm{attr}}, '_weight' unless $attr{'_weight'};
  }

  $self->{file}     = $parm{file}     or croak "No file specified";
  if (-d  $self->{file} or !mkdir($self->{file}, 0775)) {
    croak "Could not 'mkdir $self->{file}': $!\n";
  }
  $self->{name}     = $parm{name}     or croak "No name specified";
  $self->{attr}     = $parm{attr}     or croak "No attributes specified";
  $self->{djk}      = $parm{djk}      if defined $parm{djk};
  $self->{layout}   = $parm{layout} || new WAIT::Parse::Base;
  $self->{access}   = $parm{access} if defined $parm{access};
  $self->{nextk}    = 1;        # next record to insert; first record unused
  $self->{deleted}  = {};       # no deleted records yet
  $self->{indexes}  = {};

  bless $self, $type;
  # Call create_index() and create_index() for compatibility
  for (@{$self->{keyset}||[]}) {
    #carp "Specification of indexes at table create time is deprecated";
    $self->create_index(@$_);
  }
  while (@{$parm{invindex}||[]}) {
    # carp "Specification of inverted indexes at table create time is deprecated";
    my $att  = shift @{$parm{invindex}};
    my @spec = @{shift @{$parm{invindex}}};
    my @opt;
    
    if (ref($spec[0])) {
      carp "Secondary pipelines are deprecated\n";
      @opt = %{shift @spec};
    }
    $self->create_inverted_index(attribute => $att, pipeline  => \@spec, @opt);
  }
  $self;
  # end of backwarn compatibility stuff
}

=head2 Creating an index

  $tb->create_index('docid');

=item C<create_index>

must be called with a list of attributes. This must be a subset of the
attributes specified when the table was created. Currently this
method must be called before the first tuple is inserted in the
table!

=cut

sub create_index {
  my $self= shift;
  
  croak "Cannot create index for table aready populated"
    if $self->{nextk} > 1;
  
  require WAIT::Index;
  
  my $name = join '-', @_;
  $self->{indexes}->{$name} =
    new WAIT::Index file => $self->{file}.'/'.$name, attr => $_;
}

=head2 Creating an inverted index

  $tb->create_inverted_index
    (attribute => 'au',
     pipeline  => ['detex', 'isotr', 'isolc', 'split2', 'stop'],
     predicate => 'plain',
    );

=over 5

=item C<attribute>

The attribute to build the index on. This attribute may not be in the
set attributes specified when the table was created.

=item C<pipeline>

A piplines specification is a reference to and array of method names
(from package C<WAIT::Filter>) which are to applied in sequence to the
contents of the named attribute. The attribute name may not be in the
attribute list.

=item C<predicate>

An indication which predicate the index implements. This may be
e.g. 'plain', 'stemming' or 'soundex'. The indicator will be used for
query processing. Currently there is no standard set of predicate
names. The predicate defaults to the last member of the ppline if
omitted.

=back

Currently this method must be called before the first tuple is
inserted in the table!

=cut

sub create_inverted_index {
  my $self  = shift;
  my %parm  = @_;

  croak "No attribute specified" unless $parm{attribute};
  croak "No pipeline specified"  unless $parm{pipeline};

  $parm{predicate} ||= $parm{pipeline}->[-1];
  
  croak "Cannot create index for table aready populated"
    if $self->{nextk} > 1;
  
  require WAIT::InvertedIndex;

  # backward compatibility stuff
  my %opt = %parm;
  for (qw(attribute pipeline predicate)) {
    delete $opt{$_};
  }
  
  my $name = join '_', ($parm{attribute}, @{$parm{pipeline}});
  my $idx = new WAIT::InvertedIndex(file   => $self->{file}.'/'.$name,
                                    filter => [@{$parm{pipeline}}], # clone
                                    name   => $name,
                                    attr   => $parm{attribute},
                                    %opt, # backward compatibility stuff
                                   );
  # We will have to use $parm{predicate} here
  push @{$self->{inverted}->{$parm{attribute}}}, $idx;
}

sub dir {
  $_[0]->{file};
}

=head2 C<$tb-E<gt>layout>

Returns the reference to the associated parser object.

=cut

sub layout { $_[0]->{layout} }

=head2 C<$tb-E<gt>fields>

Returns the array of attribute names.

=cut


sub fields { keys %{$_[0]->{inverted}}}

=head2 C<$tb-E<gt>drop>

Must be called via C<WAIT::Database::drop_table>

=cut

sub drop {
  my $self = shift;
  if ((caller)[0] eq 'WAIT::Database') { # database knows about this
    $self->close;               # just make sure
    my $file = $self->{file};

    for (values %{$self->{indexes}}) {
      $_->drop;
    }
    unlink "$file/records";
    ! (!-e $file or rmdir $file);
  } else {
    croak ref($self)."::drop called directly";
  }
}

sub mrequire ($) {
  my $module = shift;

  $module =~ s{::}{/}g;
  $module .= '.pm';
  require $module;
}

sub open {
  my $self = shift;
  my $file = $self->{file} . '/records';

  mrequire ref($self);           # that's tricky eh?
  if (defined $self->{'layout'}) {
    mrequire ref($self->{'layout'});
  }
  if (defined $self->{'access'}) {
    mrequire ref($self->{'access'});
  }
  if (exists $self->{indexes}) {
    require WAIT::Index;
    for (values %{$self->{indexes}}) {
      $_->{mode} = $self->{mode};
    }
  }
  if (exists $self->{inverted}) {
    my ($att, $idx);
    for $att (keys %{$self->{inverted}}) {
      for $idx (@{$self->{inverted}->{$att}}) {
        $idx->{mode} = $self->{mode};
      }
    }
    require WAIT::InvertedIndex;
  }
  unless (defined $self->{dbh}) {
    if ($USE_RECNO) {
      $self->{dbh} = tie(@{$self->{db}}, 'DB_File', $file,
                         $self->{mode}, 0664, $DB_RECNO);
    } else {
      $self->{dbh} =
        tie(%{$self->{db}}, 'DB_File', $file,
                         $self->{mode}, 0664, $DB_BTREE);
    }
  }
  $self;
}

sub fetch_extern {
  my $self  = shift;

  print "#@_", $self->{'access'}->{Mode}, "\n";
  if (exists $self->{'access'}) {
    mrequire ref($self->{'access'});
    $self->{'access'}->FETCH(@_);
  }
}

sub fetch_extern_by_id {
  my $self  = shift;

  $self->fetch_extern($self->fetch(@_));
}

sub _find_index {
  my $self  = shift;
  my (@att) = @_;
  my %att;
  my $name;
  
  @att{@att} = @att;

  KEY: for $name (keys %{$self->{indexes}}) {
      my @iat = split /-/, $name;
      for (@iat) {
        next KEY unless exists $att{$_};
      }
      return $self->{indexes}->{$name};
    }
  return undef;
}

sub have {
  my $self  = shift;
  my %parm  = @_;

  my $index = $self->_find_index(keys %parm);
  croak "No index found" unless $index;
  defined $self->{db} or $self->open;
  return $index->have(@_);
}

sub insert {
  my $self  = shift;
  my %parm  = @_;

  defined $self->{db} or $self->open;

  my $tuple = join($;, map($parm{$_} || '', @{$self->{attr}}));
  my $key;
  my @deleted = keys %{$self->{deleted}};

  if (@deleted) {
    $key = pop @deleted;
    delete $self->{deleted}->{$key};
  } else {
    $key = $self->{nextk}++;
  }
  if ($USE_RECNO) {
    $self->{db}->[$key] = $tuple;
  } else {
    $self->{db}->{$key} = $tuple;
  }
  for (values %{$self->{indexes}}) {
    unless ($_->insert($key, %parm)) {
      # duplicate key, undo changes
      if ($key == $self->{nextk}-1) {
        $self->{nextk}--;
      } else {
        $self->{deleted}->{$key}=1;
      }
      my $idx;
      for $idx (values %{$self->{indexes}}) {
        last if $idx eq $_;
        $idx->remove($key, %parm);
      }
      return undef;
    } 
  }
  if (defined $self->{inverted}) {
    my $att;
    for $att (keys %{$self->{inverted}}) {
      if (defined $parm{$att}) {
        map $_->insert($key, $parm{$att}), @{$self->{inverted}->{$att}};
        #map $_->sync, @{$self->{inverted}->{$att}}
      }
    }
  }
  $key
}

sub sync {
  my $self  = shift;
  
  for (values %{$self->{indexes}}) {
    map $_->sync, $_;
  }
  if (defined $self->{inverted}) {
    my $att;
    for $att (keys %{$self->{inverted}}) {
      map $_->sync, @{$self->{inverted}->{$att}}
    }
  }
}

sub fetch {
  my $self  = shift;
  my $key   = shift;

  return () if exists $self->{deleted}->{$key};
  
  defined $self->{db} or $self->open;
  if ($USE_RECNO) {
    $self->unpack($self->{db}->[$key]);
  } else {
    $self->unpack($self->{db}->{$key});
  }
}

sub delete_by_key {
  my $self  = shift;
  my $key   = shift;

  return $self->{deleted}->{$key} if defined $self->{deleted}->{$key};
  my %tuple = $self->fetch($key);
  for (values %{$self->{indexes}}) {
    $_->delete($key, %tuple);
  }
  if (defined $self->{inverted}) {
    # User *must* provide the full record for this or the entries
    # in the inverted index will not be removed
    %tuple = (%tuple, @_);
    my $att;
    for $att (keys %{$self->{inverted}}) {
      if (defined $tuple{$att}) {
        map $_->delete($key, $tuple{$att}), @{$self->{inverted}->{$att}}
      }
    }
  }
  ++$self->{deleted}->{$key};
}

sub delete {
  my $self  = shift;
  my $tkey = $self->have(@_);

  defined $tkey && $self->delete_by_key($tkey, @_);
}

sub unpack {
  my $self = shift;
  my $tuple = shift;

  my $att;
  my @result;
  my @tuple = split /$;/, $tuple;

  for $att (@{$self->{attr}}) {
    push @result, $att, shift @tuple;
  }
  @result;
}

sub close {
  my $self = shift;

  if (exists $self->{'access'}) {
    eval {$self->{'access'}->close}; # dont bother if not opened
  }
  for (values %{$self->{indexes}}) {
    $_->close();
  }
  if (defined $self->{inverted}) {
    my $att;
    for $att (keys %{$self->{inverted}}) {
      if ($] > 5.003) {         # avoid bug in perl up to 5.003_05
        my $idx;
        for $idx (@{$self->{inverted}->{$att}}) {
          $idx->close;
        }
      } else {
        map $_->close(), @{$self->{inverted}->{$att}};
      }
    }
  }
  if ($self->{dbh}) {
    delete $self->{dbh};

    if ($USE_RECNO) {
      untie @{$self->{db}};
    } else {
      untie %{$self->{db}};
    }
    delete $self->{db};
  }

  1;
}

sub open_scan {
  my $self = shift;
  my $code = shift;

  $self->{dbh} or $self->open;
  require WAIT::Scan;
  new WAIT::Scan $self, $self->{nextk}-1, $code;
}

sub open_index_scan {
  my $self = shift;
  my $attr = shift;
  my $code = shift;
  my $name = join '-', @$attr;

  if (defined $self->{indexes}->{$name}) {
    $self->{indexes}->{$name}->open_scan($code);
  } else {
    croak "No such index '$name'";
  }
}

eval {sub WAIT::Query::Raw::new} unless defined \&WAIT::Query::Raw::new;

sub prefix {
  my ($self , $attr, $prefix) = @_;
  my %result;

  defined $self->{db} or $self->open; # require layout

  for (@{$self->{inverted}->{$attr}}) {
    my $result = $_->prefix($prefix);
    if (defined $result) {
      $result{$_->name} = $result;
    }
  }
  bless \%result, 'WAIT::Query::Raw';
}

sub intervall {
  my ($self, $attr, $lb, $ub) = @_;
  my %result;

  defined $self->{db} or $self->open; # require layout

  for (@{$self->{inverted}->{$attr}}) {
    my $result = $_->intervall($lb, $ub);
    if (defined $result) {
      $result{$_->name} = $result;
    }
  }
  bless \%result, 'WAIT::Query::Raw';
}

sub search {
  my $self = shift;
  my $attr = shift;
  my $cont = shift;
  my $raw  = shift;
  my %result;

  defined $self->{db} or $self->open; # require layout

  if ($raw) {
    for (@{$self->{inverted}->{$attr}}) {
      my $name = $_->name;
      if (exists $raw->{$name} and @{$raw->{$name}}) {
        my $scale = 1/scalar(@{$raw->{$name}});
        my %r = $_->search_raw(@{$raw->{$name}});
        my ($key, $val);
        while (($key, $val) = each %r) {
          if (exists $result{$key}) {
            $result{$key} += $val*$scale;
          } else {
            $result{$key}  = $val*$scale;
          }
        }
      }
    }
  }
  if (defined $cont and $cont ne '') {
    for (@{$self->{inverted}->{$attr}}) {
      my %r = $_->search($cont);
      my ($key, $val);
      while (($key, $val) = each %r) {
        if (exists $result{$key}) {
          $result{$key} += $val;
        } else {
          $result{$key}  = $val;
        }
      }
    }
  }
  # sanity check for deleted documents.
  # this should not be necessary !@#$
  for (keys %result) {
    delete $result{$_} if $self->{deleted}->{$_}
  }
  %result;
}

sub hilight_positions {
  my ($self, $attr, $text, $query, $raw)  = @_;
  my %pos;

  if (defined $raw) {
    for (@{$self->{inverted}->{$attr}}) {
      my $name = $_->name;
      if (exists $raw->{$name}) {
        my %qt;
        grep $qt{$_}++, @{$raw->{$name}};
        for ($_->parse_pos($text)) {
          if (exists $qt{$_->[0]}) {
            $pos{$_->[1]} = max($pos{$_->[1]}, length($_->[0]));
          }
        }
      }
    }
  }
  if (defined $query) {
    for (@{$self->{inverted}->{$attr}}) {
      my %qt;

      grep $qt{$_}++, $_->parse($query);
      for ($_->parse_pos($text)) {
        if (exists $qt{$_->[0]}) {
          if (exists $pos{$_->[1]}) { # perl -w ;-)
            $pos{$_->[1]} = max($pos{$_->[1]}, length($_->[0]));
          } else {
            $pos{$_->[1]} = length($_->[0]);
          }
        }
      }
    }
  }

  \%pos;
}

sub hilight {
  my ($tb, $text, $query, $raw) = @_;
  my $type = $tb->layout();
  my @result;

  $query ||= {};
  $raw   ||= {};
  my @ttxt = $type->tag($text);
  while (@ttxt) {
    no strict 'refs';
    my %tag = %{shift @ttxt};
    my $txt = shift @ttxt;
    my $fld;

    my %hl;
    for $fld (grep defined $tag{$_}, keys %$query, keys %$raw) {
      my $hp = $tb->hilight_positions($fld, $txt,
                                      $query->{$fld}, $raw->{$fld});
      for (keys %$hp) {
        if (exists $hl{$_}) {   # -w ;-(
          $hl{$_} = max($hl{$_}, $hp->{$_});
        } else {
          $hl{$_} = $hp->{$_};
        }
      }
    }
    my $pos;
    my $qt = {_qt => 1, %tag};
    my $pl = \%tag;
    my $last = length($txt);
    my @tmp;
    for $pos (sort {$b <=> $a} keys %hl) {
      unshift @tmp, $pl, substr($txt,$pos+$hl{$pos},$last-$pos-$hl{$pos});
      unshift @tmp, $qt, substr($txt,$pos,$hl{$pos});
      $last = $pos;
    }
    push @result, $pl, substr($txt,0,$last);
    push @result, @tmp;
  }
  @result;                      # no speed necessary
}

1;


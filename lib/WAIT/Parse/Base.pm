#                              -*- Mode: Cperl -*- 
# Base.pm -- 
# ITIID           : $ITI$ $Header $__Header$
# Author          : Ulrich Pfeifer
# Created On      : Fri Sep  6 09:50:53 1996
# Last Modified By: Ulrich Pfeifer
# Last Modified On: Sun Nov 22 18:44:42 1998
# Language        : CPerl
# Update Count    : 23
# Status          : Unknown, Use with caution!
# 
# Copyright (c) 1996-1997, Ulrich Pfeifer
# 

package WAIT::Parse::Base;
use Carp;

sub new {
  my $type = shift;

  bless {}, ref($type) || $type;
}

sub split {
  my $self = shift;
  my %result;
  my @in = $self->tag(@_);

  while (@in) {
    my $tags = shift @in;
    my $text = shift @in;
    my @tags = grep /^[^_]/, keys %$tags;
    for my $field (@tags) {
      if (exists $result{$field}) { #  make perl -w happy
        $result{$field} .= ' ' . $text;
      } else {
        $result{$field} = $text; 
      }
    }
  }
  return \%result;              # we go for speed
}

sub tag {
  ({text => 1}, $_[0]);
}

1;

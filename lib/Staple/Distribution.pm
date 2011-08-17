package Staple::Distribution;

#
# Copyright (C) 2007-2011 Hebrew University Of Jerusalem, Israel
# See the LICENSE file.
#
# Author: Yair Yarom <irush@cs.huji.ac.il>
#

=head1 NAME

  Staple::Distribution - Represents a staple distribution

=head1 DESCRIPTION

Staple::Distribution module. subclass of Staple::Groups and Staple::Node

=head1 METHODS

=over

=cut

use strict;
use warnings;
use Staple::Node;
use Staple::Group;
use Staple::Misc;
require Exporter;

our @ISA = ("Staple::Group");
our $VERSION = '007snap';

=item B<new(I<\%attr>, [I<\%attr> [...]])>

Creates a new distribution with attributes (see B<init> for list of attributes). If
several attribute references exists, will return a list of new distributions. Name
attribute must be valid, or error string is returned.

Returns a list of Distributions and error strings if several attribute sets are
given

=cut

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my @attrs = @_;
    my @selves = ();
    @attrs = ({}) unless @attrs; 
    foreach my $attr (@attrs) {
        my $self = {};
        bless $self, $proto;
        $self->init($attr);
        $self = $self->{error} if $self->{error};
        push @selves, $self;
    }
    return @selves;
}

=item B<init(I<\%attr>)>

(re)initialize the distribution. Reset any previous attributes and sets up
according to attrs. returns undef and sets the error if name is
invalid. Attributes can be:

=over

=over

=item I<name>   - The name of the distribution.

=item I<path>   - The full path of the distribution (filesystem)

=back

=back

=cut

sub init {
    my $self = shift;
    my %attr = %{$_[0]};
    $self->{error} = "";
    
    $self->name($attr{name});
    return undef if $self->{error} or $self->{error} = $self->{error} = invalidDistribution($self->name());
    
    # XXX to be removed
    $self->{type} = "distribution";
    $self->{path} = $attr{path};
}

=item B<type()>

returns 'distribution'

=cut

sub type {
    return "distribution";
}

=item B<name(I<name>)>

Sets this distribution's name (string), and returns the previous name. If
I<name> is undef don't change it, just return the current one.

If I<name> is invalid, returns undef and sets the error.

=cut

sub name {
    my $self = shift;
    my $name = shift;
    my $active = undef;
    return $self->{name} unless defined $name;
    return undef if $self->{error} = invalidDistribution($name);
    return $self->param("name", $name);
}

=item B<path(I<path>)>

Sets this distribution's path (string), and returns the previous path. If
I<path> is undef don't change it, just return the current one. Used internally
by some DBs.

=cut

sub path {
    my $self = shift;
    return $self->param("path", shift);
}

################################################################################
# Internals
################################################################################

1;

__END__

=back

=head1 SEE ALSO

L<Staple> - Staple main module.

L<Staple::Node> - Base class for all nodes

=head1 AUTHOR

Yair Yarom, E<lt>irush@cs.huji.ac.ilE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2007-2011 Hebrew University Of Jerusalem, Israel
See the LICENSE file.

=cut

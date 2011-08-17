package Staple::Configuration;

#
# Copyright (C) 2007-2011 Hebrew University Of Jerusalem, Israel
# See the LICENSE file.
#
# Author: Yair Yarom <irush@cs.huji.ac.il>
#

=head1 NAME

  Staple::Configuration - Represents a staple configuration

=head1 DESCRIPTION

Staple::Configuration module. subclass of Staple::Node

=head1 METHODS

=over

=cut

use strict;
use warnings;
use Staple::Node;
use Staple::Misc;
require Exporter;

our @ISA = ("Staple::Node");
our $VERSION = '006';

=item B<new(I<\%attr>, [I<\%attr> [...]])>

Creates a new configuration with attributes (see B<init> for list of
attributes). If several attribute references exists, will return a list of new
Configurations. Name attribute must be valid, or error string is returned.

Returns a list of Configurations and error strings if several attribute sets are given

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

(re)initialize the configuration. Reset any previous attributes and sets up
according to attrs. returns undef and sets the error if name is
invalid. Attributes can be:

=over

=over

=item I<name>   - The name of the configuration, if name is prefixed with + or -, it sets as the active attribute.

=item I<path>   - The full path of the configuration (filesystem)

=item I<dist>   - The distribution

=item I<active> - Whether the configuration is active (1) or not (0). defaults to 1 if not defined (and not name prefixed)

=item I<group>  - Originating group hash (if any)

=back

=back

=cut

sub init {
    my $self = shift;
    my %attr = %{$_[0]};
    $self->{error} = "";
    
    # active might be set by name
    delete $self->{active};
    $self->name($attr{name});
    return undef if $self->{error} or $self->{error} = $self->{error} = invalidConfiguration($self->name());
    
    # set if givena as parameter, otherwise set to 1 if not set by name
    $self->active(exists $attr{active} ? $attr{active} : exists $self->{active} ? $self->{active} : 1);
    $self->{dist} = $attr{dist};
    $self->{group} = $attr{group};
    
    # XXX to be removed
    $self->{type} = "configuration";
    $self->{path} = $attr{path};
}

=item B<type()>

returns 'configuration'

=cut

sub type {
    return "configuration";
}

=item B<name(I<name>)>

Sets this configuration's name (string), and returns the previous name. If
I<name> is undef don't change it, just return the current one. If I<name> is
prefixed with + or -, it sets the active attribute.

If I<name> is invalid, returns undef and sets the error.

=cut

sub name {
    my $self = shift;
    my $name = shift;
    my $active = undef;
    return $self->{name} unless defined $name;
    if ($name =~ m/^([+-])(.*)$/) {
        $active = $1 eq '+';
        $name = $2;
    }
    return undef if $self->{error} = invalidConfiguration($name);
    $self->active($active) if defined $active;
    return $self->param("name", $name);
}

=item B<isCommon()>

Returns true if this configuration is common (name starts with common/)

=cut

sub isCommon {
    my $self = shift;
    return index($self->{name}, "common/") == 0
}

=item B<dist(I<dist>)>

Sets this configuration's distribution (string), and returns the previous
distribution. If I<dist> is undef don't change it, just return the current one.

=cut

sub dist {
    my $self = shift;
    return $self->param("dist", shift);
}

=item B<active(I<active>)>

Sets this configuration's active state, and returns the previous one. if
I<active> is undef don't change it, just return the current one.

I<active> can be 0,1,yes,no,+,-. The returned value is always 0 or 1.

=cut

sub active {
    my $self = shift;
    my $active = shift;
    return $self->{active} unless defined $active;
    $active = lc($active);
    my %actives = (1     => "1",
                   "yes" => "1",
                   "+"   => "1",
                   0     => "0",
                   "no"  => "0",
                   "-"   => "0");
    $active = exists $actives{$active} ? $actives{$active} : $active ? 1 : 0;
    return $self->param("active", $active);
}

=item B<group(I<group>)>

Sets this configuration's originating group (hash), and returns the previous
group. If I<group> is undef don't change it, just return the current one.

=cut

sub group {
    my $self = shift;
    return $self->param("group", shift);
}

=item B<path(I<path>)>

Sets this configuration's path (string), and returns the previous path. If
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

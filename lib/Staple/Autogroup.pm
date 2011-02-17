package Staple::Autogroup;

#
# Copyright (C) 2007-2010 Hebrew University Of Jerusalem, Israel
# See the LICENSE file.
#
# Author: Yair Yarom <irush@cs.huji.ac.il>
#

=head1 NAME

  Staple::Autogroup - Represents an autogroup script

=head1 DESCRIPTION

Staple::Autogroup module. subclass of Staple::Setting

=head1 METHODS

=over

=cut

use strict;
use warnings;
use Staple::Setting;
require Exporter;

our @ISA = ("Staple::Setting");
our $VERSION = '006snap';

=item B<new(I<\%attr>, [I<\%attr> [...]])>

Creates a new autogroup with attributes (see B<init> for list of
attributes). If several attribute references exists, will return a list of new
Autogroups.

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
        push @selves, $self;
    }
    return @selves;
}

=item B<init(I<\%attr>)>

(re)initialize the autogroup. Reset any previous attributes and sets up
according to attrs. Attributes can be:

=over

=over

=item I<name>          - The name of the autogroup

=item I<source>        - A file containing the data for this autogroup. The file
                         will be read when necessary (not at initialization).

=item I<data>          - Data for this autogroup. Ignored if I<source> is available.

=item I<configuration> - The configuration of this autogroup (hash ref)

=item I<order>         - The location of this autogroup in the local order (a number)

=item I<critical>      - Whether this autogroup should invoke critical action when it fails (0 or 1)

=item I<tokens>        - Whether this autogroup should pass through tokens substitution before running (0 or 1)

=item I<note>          - A note. Used as comment, doesn't effect anything.

=back

=back

=cut

sub init {
    my $self = shift;
    my %attr = %{$_[0]};
    $self->{name} = $attr{name};
    $self->{source} = $attr{source};
    $self->{data} = $attr{data};
    $self->{configuration} = $attr{configuration};
    $self->{order} = $attr{order};
    $self->{critical} = $attr{critical};
    $self->{tokens} = $attr{tokens};
    $self->{note} = $attr{note};
    $self->{error} = "";
}

=item B<name(I<name>)>

Sets this autogroup's name (string), and returns the previous name. If I<name>
is undef don't change it, just return the current one.

=cut

sub name {
    my $self = shift;
    return $self->param("name", shift);
}

=item B<order(I<order>)>

Sets this autogroup's order (number) in the configuration, and returns the
previous order. If I<order> is undef don't change it, just return the current
one.

=cut

sub order {
    my $self = shift;
    return $self->param("order", shift);
}

=item B<critical(I<critial>)>

Sets this autogroup's critical (boolean) which if set, causes a critical action
on failure. Returns the previous critical. If I<critical> is undef don't change
it, just return the current one.

=cut

sub critical {
    my $self = shift;
    return $self->param("critical", shift);
}

=item B<tokens(I<tokens>)>

Sets this autogroup's tokens attribute (boolean) which if set, causes a token
substitution to pass through the script before running it. Returns the previous
tokens. If I<tokens> is undef don't change it, just return the current one.

=cut

sub tokens {
    my $self = shift;
    return $self->param("tokens", shift);
}

=item B<description(I<level>)>

Returns a string describing this autogroup. I<level> will specify the format:

=over

=item 0 - Default, one line: #. name (critical? tokens? file|data)

=item 1 - Long, all available information (excluding "data" if available)

=back

=cut

sub description {
    my $self = shift;
    my $level = shift;
    my $result = "";
    if ($level) {
        $result .= "$self->{name}:\n";
        $result .= "  source => $self->{source}\n";
        $result .= "  data => ".($self->{data} ? "(trimmed)" : "(undef)")."\n";
        $result .= "  name => $self->{name}\n";
        $result .= "  configuration => ".($self->{configuration} ? $self->{configuration}->name() : "(undef)")."\n";
        $result .= "  order => $self->{order}\n";
        $result .= "  critical => $self->{critical}\n";
        $result .= "  tokens => $self->{tokens}\n";
        $result .= "  note => ".($self->{note} ? $self->{note} : "(undef)")."\n";
    } else {
        $result .= "$_->{order}. $_->{name}\t(".($_->{critical} ? "critical " : "").($_->{tokens} ? "tokened " : "").($_->{source} ? "file" : "data").")\n";
    }
    return $result;
}

# no stage for autogroup
sub stage {
    return undef;
}

################################################################################
# Internals
################################################################################

1;

__END__

=back

=head1 SEE ALSO

L<Staple> - Staple main module.

L<Staple::Setting> - Base class for all settings

=head1 AUTHOR

Yair Yarom, E<lt>irush@cs.huji.ac.ilE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2007-2010 Hebrew University Of Jerusalem, Israel
See the LICENSE file.

=cut

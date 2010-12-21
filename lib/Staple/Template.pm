package Staple::Template;

#
# Copyright (C) 2007-2010 Hebrew University Of Jerusalem, Israel
# See the LICENSE file.
#
# Author: Yair Yarom <irush@cs.huji.ac.il>
#

=head1 NAME

  Staple::Template - Represents a staple template

=head1 DESCRIPTION

Staple::Template module.

=head1 METHODS

=over

=cut

use strict;
use warnings;

=item B<new(I<\%attr>, [I<\%attr> [...]])>

Creates a new template with attributes (see B<init> for list of attributes). If
several attribute references exists, will return a list of new Templates.

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

(re)initialize the template. Reset any previous attributes and sets up
according to attrs. Attributes can be:

=over

=over

=item I<source>        - A file containing the data for this template. The file
                         will be read when necessary (not at initialization).

=item I<data>          - Data for this template. Ignored if I<source> is available.

=item I<destination>   - The destination path (where to copy to)

=item I<stage>         - The stage this template should be copied (mount, sysinit, or final)

=item I<configuration> - The configuration of this template (hash ref)

=item I<mode>          - The mode (octal) for the template

=item I<gid>           - The gid (or group) for the template

=item I<uid>           - The uid (or username) for the template

=item I<note>          - A note. Used as comment, doesn't effect anything.

=back

=back

=cut

sub init {
    my $self = shift;
    my %attr = %{$_[0]};
    $self->{source} = $attr{source};
    $self->{data} = $attr{data};
    $self->{destination} = $attr{destination};
    $self->{stage} = $attr{stage};
    $self->{configuration} = $attr{configuration};
    $self->{mode} = $attr{mode};
    $self->{gid} = $attr{gid};
    $self->{uid} = $attr{uid};
    $self->{note} = $attr{note};
    $self->{error} = "";
}

=item B<configuration(I<configuration>)>

Sets this templates' configuration (hash), and returns the previous
configuration. If I<configuration> is undef don't change it, just return the
current configuration.

=cut

sub configuration {
    param(shift, "configuration", shift);
}

=item B<destination(I<destination>)>

Sets this templates' destination (string), and returns the previous
destination. If I<destination> is undef don't change it, just return the
current one.

=cut

sub destination {
    param(shift, "destination", shift);
}

=item B<stage(I<stage>)>

Sets this templates' stage (string), and returns the previous stage. If
I<stage> is undef don't change it, just return the current one.

=cut

sub stage {
    param(shift, "stage", shift);
}

=item B<note(I<note>)>

Sets this templates' note (string), and returns the previous note. If I<note>
is undef don't change it, just return the current one.

=cut

sub note {
    param(shift, "note", shift);
}


=item B<mode(I<mode>)>

Sets this templates' mode (octal), and returns the previous mode. If I<mode> is
undef don't change it, just return the current one.

=cut

sub mode {
    param(shift, "mode", shift);
}

=item B<uid(I<uid>)>

Sets this templates' uid (number or username), and returns the previous
uid. If I<uid> is undef don't change it, just return the current one.

=cut

sub uid {
    param(shift, "uid", shift);
}

=item B<gid(I<gid>)>

Sets this templates' gid (group or number), and returns the previous gid. If
I<gid> is undef don't change it, just return the current one.

=cut

sub gid {
    param(shift, "gid", shift);
}

=item B<data(I<data>)>

If I<data> is defined, changes the templates' data and empty the
source. Returns the previous data, i.e. if source is defined, read it, if not
then the previous data.

=cut

sub data {
    my $self = shift;
    my $indata = shift;
    my $outdata = $self->readSource();
    return undef if $self->{error};
    $outdata ||= $self->{data};
    if (defined $indata) {
        delete $self->{source};
        $self->{data} = $indata;
    }
    return $outdata;
}

=item B<readSource()>

Reads the I<source> and returns it's data. Returns undef on error. Returns
empty string if doesn't have source.

=cut

sub readSource {
    my $self = shift;
    $self->{error} = "";
    my $out = "";
    if ($self->{source}) {
        unless (open(FILE, "<$self->{source}")) {
            $self->{error} = "Can't open template source for reading \"$self->{source}\": $!";
            return undef;
        }
        $out = join "", <FILE>;
        close(FILE);
    }
    return $out;
}

=item B<useData()>

Fills the data of this template from its source and remove the source from this
template (doesn't delete the file). If doesn't have source, but has data, does
nothing. Returns 1 on success or 0 on failure (failed open source).

=cut

sub useData {
    my $self = shift;
    my $data = $self->readSource();
    return 0 if $self->{error};
    $self->{data} = $data;
    delete $self->{source};
    return 1;
}

=item B<error()>

Gets the last error or an empty string.

=cut

sub error {
    my $self = shift;
    return $self->{error};
}

################################################################################
# Internals
################################################################################

# input: (self), name, value
# output: old value
# changed to new value if defined
sub param {
    my $self = shift;
    my $key = shift;
    my $value = shift;
    my $old = $self->{$key};
    $self->{$key} = $value if $value;
    return $old;
}

1;

__END__

=back

=head1 SEE ALSO

L<Staple> - Staple main module.

=head1 AUTHOR

Yair Yarom, E<lt>irush@cs.huji.ac.ilE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2007-2010 Hebrew University Of Jerusalem, Israel
See the LICENSE file.

=cut

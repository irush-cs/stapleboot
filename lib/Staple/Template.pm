package Staple::Template;

#
# Copyright (C) 2007-2011 Hebrew University Of Jerusalem, Israel
# See the LICENSE file.
#
# Author: Yair Yarom <irush@cs.huji.ac.il>
#

=head1 NAME

  Staple::Template - Represents a staple template

=head1 DESCRIPTION

Staple::Template module. Subclass of Staple::Setting

=head1 METHODS

=over

=cut

use strict;
use warnings;
use Staple::Misc;
use Staple::Setting;
use Clone qw(clone);
require Exporter;

our @ISA = ("Staple::Setting");
our $VERSION = '007snap';

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

=item B<destination(I<destination>)>

Sets this templates' destination (string), and returns the previous
destination. If I<destination> is undef don't change it, just return the
current one.

=cut

sub destination {
    my $self = shift;
    return $self->param("destination", shift);
}

=item B<mode(I<mode>)>

Sets this templates' mode (octal), and returns the previous mode. If I<mode> is
undef don't change it, just return the current one.

=cut

sub mode {
    my $self = shift;
    return $self->param("mode", shift);
}

=item B<uid(I<uid>)>

Sets this templates' uid (number or username), and returns the previous
uid. If I<uid> is undef don't change it, just return the current one.

=cut

sub uid {
    my $self = shift;
    return $self->param("uid", shift);
}

=item B<gid(I<gid>)>

Sets this templates' gid (group or number), and returns the previous gid. If
I<gid> is undef don't change it, just return the current one.

=cut

sub gid {
    my $self = shift;
    return $self->param("gid", shift);
}

=item B<apply(I<tokens hash>, I<rootdir>)>

Applies this template with the given I<tokens>, into I<rootdir>. Returns the
template source (or destination) on success or undef on error (and sets the
error()).

=cut

sub apply {
    my $self = shift;
    my $tokens = clone(shift);
    my $rootdir = shift;

    my $confname = $self->configuration()->name();
    my $data = $self->data();
    if ($self->error()) {
        return undef;
    }

    $tokens->{__AUTO_CONFIGURATION__} = {key => "__AUTO_CONFIGURATION__",
                                         value => $confname,
                                         raw => $confname,
                                         type => "static",
                                         source => "auto"};
    $tokens->{__AUTO_STAGE__} = {key => "__AUTO_STAGE__",
                                 value => $self->stage(),
                                 raw => $self->stage(),
                                 type => "static",
                                 source => "auto"};
    $data = applyTokens($data, $tokens);
    delete $tokens->{__AUTO_CONFIGURATION__};
    delete $tokens->{__AUTO_STAGE__};
    my $destination = "$rootdir".$self->destination();
    if ($self->destination() =~ m@^/__AUTO_TMP__/@ and
        exists $tokens->{__AUTO_TMP__}) {
        $destination = $self->destination();
        $destination =~ s@^/__AUTO_TMP__@$tokens->{__AUTO_TMP__}->{value}@;
    }
    $destination = fixPath($destination);

    my @dirs = splitData($destination);
    pop @dirs;
    my $configurationPath = $self->configuration()->path()."/templates/$self->{stage}";
    foreach my $dir (@dirs) {
        unless (-e "$dir") {
            mkdir "$dir";
            (my $mode, my $uid, my $gid) = (stat("$configurationPath$dir"))[2,4,5];
            chown $uid, $gid, "$dir";
            chmod $mode & 07777, "$dir";
        }
    }

    if (open(FILE, ">$destination")) {
        print FILE $data;
        close(FILE);
        #(my $mode, my $uid, my $gid) = (stat("$template->{source}"))[2,4,5];
        #chown $uid, $gid, "$rootDir$template->{destination}";
        #chmod $mode & 07777, "$rootDir$template->{destination}";
        chown $self->uid(), $self->gid(), "$destination";
        chmod $self->mode(), "$destination";
        if ($self->source()) {
            return $self->source();
        } else {
            return $destination;
        }
    } else {
        $self->{error} = "Can't write to $destination: $!";
        return undef;
    }
}

=item B<description(I<level>)>

Returns a string describing this template. I<level> will specify the format:

=over

=item 0 - Default, one line: (uid:gid mode file|data): destination

=item 1 - Long, all available information (excluding "data" if available)

=back

=cut

sub description {
    my $self = shift;
    my $level = shift;
    my $result = "";
    if ($level) {
        $result .= "$self->{destination}:\n";
        $result .= "  source => $self->{source}\n";
        $result .= "  data => ".($self->{data} ? "(trimmed)" : "(undef)")."\n";
        $result .= "  destination => $self->{destination}\n";
        $result .= "  stage => $self->{stage}\n";
        $result .= "  configuration => ".($self->{configuration} ? $self->{configuration}->name() : "(undef)")."\n";
        $result .= "  mode => ".sprintf("%04o", $self->{mode})."\n";
        $result .= "  gid => $self->{gid}\n";
        $result .= "  uid => $self->{uid}\n";
        $result .= "  note => ".($self->{note} ? $self->{note} : "(undef)")."\n";
    } else {
        $result .= "($self->{uid}:$self->{gid} ".sprintf("%04o", $self->mode())." ".($self->source() ? "file" : "data")."): ".$self->{destination}."\n";
    }
    return $result;
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

Copyright (C) 2007-2011 Hebrew University Of Jerusalem, Israel
See the LICENSE file.

=cut

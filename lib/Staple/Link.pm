package Staple::Link;

#
# Copyright (C) 2007-2012 Hebrew University Of Jerusalem, Israel
# See the LICENSE file.
#
# Author: Yair Yarom <irush@cs.huji.ac.il>
#

=head1 NAME

  Staple::Link - Represents a link

=head1 DESCRIPTION

Staple::Link module. Subclass of Staple::Template

=head1 METHODS

=over

=cut

use strict;
use warnings;
use Staple::Misc;
use Staple::Template;
use Clone qw(clone);
require Exporter;

our @ISA = ("Staple::Template");
our $VERSION = '0.2.x';

=item B<new(I<\%attr>, [I<\%attr> [...]])>

Creates a new link with attributes (see B<init> for list of attributes). If
several attribute references exists, will return a list of new Links.

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

(re)initialize the link. Reset any previous attributes and sets up according to
attrs. Attributes can be:

=over

=over

=item I<source>        - The source. Where the link will point to.

=item I<destination>   - The destination path (where the link will be created)

=item I<stage>         - The stage this link should be created (mount, sysinit, or final)

=item I<configuration> - The configuration of this link (hash ref)

=item I<note>          - A note. Used as comment, doesn't effect anything.

=back

=back

=cut

sub init {
    my $self = shift;
    my %attr = %{$_[0]};
    $self->{source} = $attr{source};
    $self->{destination} = $attr{destination};
    $self->{stage} = $attr{stage};
    $self->{configuration} = $attr{configuration};
    $self->{note} = $attr{note};
    $self->{error} = "";
}

=item B<destination(I<destination>)>

Sets this link's destination (string), and returns the previous destination. If
I<destination> is undef don't change it, just return the current one.

=cut

sub destination {
    my $self = shift;
    return $self->param("destination", shift);
}

=item B<apply(I<tokens hash>, I<rootdir>)>

Create this link in I<rootdir>. The <tokens hash> is for when create the link
in __AUTO_TMP__. Returns the link destination on success or undef on error (and
sets the error()).

=cut

sub apply {
    my $self = shift;
    my $tokens = clone(shift);
    my $rootdir = shift;

    my $source = $self->source();
    if ($self->error()) {
        return undef;
    }

    my $destination = "$rootdir".$self->destination();
    if ($self->destination() =~ m@^/__AUTO_TMP__/@ and
        exists $tokens->{__AUTO_TMP__}) {
        $destination = $self->destination();
        $destination =~ s@^/__AUTO_TMP__@$tokens->{__AUTO_TMP__}->{value}@;
    }
    $destination = fixPath($destination);

    unless ($source) {
        $self->{error} = "Source for link ".$self->destination()." isn't set";
        return undef;
    }

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

    unlink "$destination" if (-e $destination);
    unless (symlink $self->source(), "$destination") {
        $self->{error} = "Can't create the link $destination: $!";
    }
}

=item B<description(I<level>)>

Returns a string describing this link. I<level> will specify the format:

=over

=item 0 - Default, one line: (link): destination -> source

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
        $result .= "  destination => $self->{destination}\n";
        $result .= "  stage => $self->{stage}\n";
        $result .= "  configuration => ".($self->{configuration} ? $self->{configuration}->name() : "(undef)")."\n";
        $result .= "  note => ".($self->{note} ? $self->{note} : "(undef)")."\n";
    } else {
        $result .= "(???:??? ???? link): ".$self->{destination}." -> ".$self->source()."\n";
    }
    return $result;
}


=item B<type()>

Returns 'link'.

=cut

sub type {
    return 'link';
}



################################################################################
# Internals
################################################################################

sub writeSource {
    my $self = shift;
    $self->{error} = "Setting::writeSource not available for Staple::Link";
    return undef;
}

sub readSource {
    my $self = shift;
    $self->{error} = "Setting::readSource not available for Staple::Link";
    return undef;
}

sub data {
    my $self = shift;
    $self->{error} = "Setting::data not available for Staple::Link";
    return undef;
}

sub useData {
    my $self = shift;
    $self->{error} = "Setting::useData not available for Staple::Link";
    return undef;
}

1;

__END__

=back

=head1 SEE ALSO

L<Staple> - Staple main module.

L<Staple::Setting> - Base class for all settings

L<Staple::Template> - Template class

=head1 AUTHOR

Yair Yarom, E<lt>irush@cs.huji.ac.ilE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2007-2012 Hebrew University Of Jerusalem, Israel
See the LICENSE file.

=cut

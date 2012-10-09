package Staple::Mount;

#
# Copyright (C) 2007-2011 Hebrew University Of Jerusalem, Israel
# See the LICENSE file.
#
# Author: Yair Yarom <irush@cs.huji.ac.il>
#

=head1 NAME

  Staple::Mount - Represents a staple mount

=head1 DESCRIPTION

Staple::Mount module. subclass of Staple::Setting. The data, stage, and note
attributes of Staple::Setting aren't used in this module. The source attribute
has a different meaning.

=head1 METHODS

=over

=cut

use strict;
use warnings;
use Staple::Setting;
require Exporter;

our @ISA = ("Staple::Setting");
our $VERSION = '0.2.x';

=item B<new(I<\%attr>, [I<\%attr> [...]])>

Creates a new mount with attributes (see B<init> for list of attributes). If
several attribute references exists, will return a list of new Mountss.

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

(re)initialize the mount. Reset any previous attributes and sets up according
to attrs. Attributes can be:

=over

=over

=item I<source>         - The device

=item I<destination>    - The mounting point, may be prefixed with + or - to set the active attribute

=item I<type>           - Mount type (eg. tmpfs, bind, etc.)

=item I<options>        - Mount options 

=item I<active>         - Whether the mount is active (1) or not (0) (default to 1)

=item I<permissions>    - Mount permissions (octal)

=item I<next>           - Next mount if this one fails (a configuration name), only works on manual mode

=item I<critical>       - Whether to declare a critical state if fails (implies manual).

=item I<configuration>  - Configuration for this mount

=item I<copySource>     - The location of the source files to copy from (no copy if empty)

=item I<copyFiles>      - The files to copy from the source (defaults to .)

=item I<copyLinks>      - Files to symlink instead of copy

=item I<copyExclude>    - The fiels to exclude from the source (default to "")

=item I<manual>         - Whether to manually mount (1) or just write an fstab entry (0)

=item I<fsck>           - Whether to run fsck (1) or not (0).

=item I<fsckCommand>    - Special fsck command for this mount (defaults to empty, only for manual mount)

=item I<fsckExitOK>     - Special fsck exit status for this mount

=back

=back

=cut

sub init {
    my $self = shift;
    my %attr = %{$_[0]};

    # active might be set by destination
    delete $self->{active};
    $self->destination($attr{destination});
    #return undef if $self->{error} or $self->{error} = $self->{error} = invalidConfiguration($self->name());
    
    # set if given as a parameter, otherwise set to 1 if not set by name
    $self->active(exists $attr{active} ? $attr{active} : exists $self->{active} ? $self->{active} : 1);

    $self->{source} = $attr{source};
    $self->{type} = $attr{type};
    $self->{options} = $attr{options};
    $self->{permissions} = $attr{permissions};
    $self->{next} = $attr{next};
    $self->{critical} = $attr{critical};
    $self->{configuration} = $attr{configuration};
    $self->{copySource} = $attr{copySource};
    $self->{copyFiles} = $attr{copyFiles};
    $self->{copyLinks} = $attr{copyLinks};
    $self->{copyExclude} = $attr{copyExclude};
    $self->{manual} = $attr{manual};
    $self->{fsck} = $attr{fsck};
    $self->{fsckCommand} = $attr{fsckCommand};
    $self->{fsckExitOK} = $attr{fsckExitOK};

    $self->{error} = "";
}


=item B<destination(I<destination>)>

Sets this mount's destination (path string), and returns the previous
destination. If I<destination> is undef don't change it, just return the
current one. If I<destination> is prefixed with + or -, it sets the active
attribute.

=cut

sub destination {
    my $self = shift;
    my $destination = shift;
    my $active = undef;
    return $self->{destination} unless defined $destination;
    if ($destination =~ m/^([+-])(.*)$/) {
        $active = $1 eq '+';
        $destination = $2;
    }
    #return undef if $self->{error} = invalidConfiguration($name);
    $self->active($active) if defined $active;
    return $self->param("destination", $destination);
}

=item B<type(I<type>)>

Sets this mount's mount type (string), and returns the previous type. If
I<type> is undef don't change it, just return the current one.

=cut

sub type {
    my $self = shift;
    return $self->param("type", shift);
}

=item B<options(I<options>)>

Sets this mount's mount options (string), and returns the previous options. If
I<options> is undef don't change it, just return the current one.

=cut

sub options {
    my $self = shift;
    return $self->param("options", shift);
}

=item B<active(I<active>)>

Sets this mount's active state, and returns the previous one. if I<active> is
undef don't change it, just return the current one.

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

=item B<permissions(I<permissions>)>

Sets this mount's permissions (octal), and returns the previous permissions. If
I<permissions> is undef don't change it, just return the current one.

=cut

sub permissions {
    my $self = shift;
    return $self->param("permissions", shift);
}

=item B<next(I<next>)>

Sets this mount's next configuration (configuration name), and returns the
previous next. If I<next> is undef don't change it, just return the current
one.

=cut

sub next {
    my $self = shift;
    return $self->param("next", shift);
}

=item B<copySource(I<copySource>)>

Sets this mount's copySource (string), and returns the previous copySource. If
I<copySource> is undef don't change it, just return the current one.

=cut

sub copySource {
    my $self = shift;
    return $self->param("copySource", shift);
}

=item B<copyFiles(I<copyFiles>)>

Sets this mount's copyFiles (string), and returns the previous copyFiles. If
I<copyFiles> is undef don't change it, just return the current one.

=cut

sub copyFiles {
    my $self = shift;
    return $self->param("copyFiles", shift);
}

=item B<copyLinks(I<copyLinks>)>

Sets this mount's copyLinks (string), and returns the previous copyLinks. If
I<copyLinks> is undef don't change it, just return the current one.

=cut

sub copyLinks {
    my $self = shift;
    return $self->param("copyLinks", shift);
}

=item B<copyExclude(I<copyExclude>)>

Sets this mount's copyExclude (string), and returns the previous
copyExclude. If I<copyExclude> is undef don't change it, just return the
current one.

=cut

sub copyExclude {
    my $self = shift;
    return $self->param("copyExclude", shift);
}

=item B<manual(I<manual>)>

Sets this mount's manual (boolean), and returns the previous manual. If
I<manual> is undef don't change it, just return the current one.

=cut

sub manual {
    my $self = shift;
    return $self->param("manual", shift);
}

=item B<fsck(I<fsck>)>

Sets this mount's fsck (boolean), and returns the previous fsck. If I<fsck> is
undef don't change it, just return the current one.

=cut

sub fsck {
    my $self = shift;
    return $self->param("fsck", shift);
}

=item B<fsckCommand(I<fsckCommand>)>

Sets this mount's fsckCommand (string), and returns the previous fsckCommand. If
I<fsckCommand> is undef don't change it, just return the current one.

=cut

sub fsckCommand {
    my $self = shift;
    return $self->param("fsckCommand", shift);
}

=item B<fsckExitOK(I<fsckExitOK>)>

Sets this mount's fsckExitOK (string), and returns the previous fsckExitOK. If
I<fsckExitOK> is undef don't change it, just return the current one.

=cut

sub fsckExitOK {
    my $self = shift;
    return $self->param("fsckExitOK", shift);
}

=item B<source(I<source>)>

Sets this mount's source (string), and returns the previous source. If
I<source> is undef don't change it, just return the current one.

=cut

sub source {
    my $self = shift;
    return $self->param("source", shift);
}

=item B<critical(I<critial>)>

Sets this mount's critical (boolean) which if set, causes a critical action on
failure. Returns the previous critical. If I<critical> is undef don't change
it, just return the current one.

=cut

sub critical {
    my $self = shift;
    return $self->param("critical", shift);
}


=item B<description(I<level>)>

Returns a string describing this mount. I<level> will specify the format:

=over

=item 0 - Default, short, one line: [+|-]destination

=item 1 - Long

=back

=cut

sub description {
    my $self = shift;
    my $level = shift;
    my $result = "";
    if ($level) {
        $result .= "$self->{destination}:\n";
        $result .= "  active => $self->{active}\n";
        $result .= "  configuration => ".($self->{configuration} ? $self->{configuration}->name() : "(undef)")."\n";
        $result .= "  copyExclude => $self->{copyExclude}\n";
        $result .= "  copyFiles => $self->{copyFiles}\n";
        $result .= "  copySource => $self->{copySource}\n";
        $result .= "  critical => $self->{critical}\n";
        $result .= "  destination => $self->{destination}\n";
        $result .= "  fsck => $self->{fsck}\n";
        $result .= "  fsckCommand => $self->{fsckCommand}\n";
        $result .= "  fsckExitOK => $self->{fsckExitOK}\n";
        $result .= "  manual => $self->{manual}\n";
        $result .= "  next => $self->{next}\n";
        $result .= "  options => $self->{options}\n";
        $result .= "  permissions => $self->{permissions}\n";
        $result .= "  source => $self->{source}\n";
        $result .= "  type => $self->{type}\n";
    } else {
        $result .= ($self->{active} ?  "+" : "-").$self->{destination};
    }
    return $result;
}

################################################################################
# Internals
################################################################################

sub writeSource {
    my $self = shift;
    $self->{error} = "Setting::writeSource not available for Staple::Mount";
    return undef;
}

sub readSource {
    my $self = shift;
    $self->{error} = "Setting::readSource not available for Staple::Mount";
    return undef;
}

sub data {
    my $self = shift;
    $self->{error} = "Setting::data not available for Staple::Mount";
    return undef;
}

sub useData {
    my $self = shift;
    $self->{error} = "Setting::data not available for Staple::Mount";
    return undef;
}

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

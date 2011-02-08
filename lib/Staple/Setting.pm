package Staple::Setting;

#
# Copyright (C) 2007-2011 Hebrew University Of Jerusalem, Israel
# See the LICENSE file.
#
# Author: Yair Yarom <irush@cs.huji.ac.il>
#

=head1 NAME

  Staple::Setting - Base class for settings (template, script, auto)

=head1 DESCRIPTION

Staple::Setting abstract class.

=head1 METHODS

=over

=cut

use strict;
use warnings;

our $VERSION = '006snap';

=item B<configuration(I<configuration>)>

Sets this setting's configuration (hash), and returns the previous
configuration. If I<configuration> is undef don't change it, just return the
current configuration.

=cut

sub configuration {
    return param(shift, "configuration", shift);
}

=item B<stage(I<stage>)>

Sets this setting's stage (string), and returns the previous stage. If I<stage>
is undef don't change it, just return the current one.

=cut

sub stage {
    return param(shift, "stage", shift);
}

=item B<note(I<note>)>

Sets this setting's note (string), and returns the previous note. If I<note> is
undef don't change it, just return the current one.

=cut

sub note {
    return param(shift, "note", shift);
}

=item B<source(I<source>)>

Sets this setting's source (full path), and returns the previous source. If
I<source> is undef don't change it, just return the current one. This deletes
the setting's data (even if the file doesn't exists).

=cut

sub source {
    my $self = shift;
    my $insource = shift;
    delete $self->{data} if defined $insource;
    return $self->param("source", $insource);
}

=item B<writeSource(I<source>)>

Writes the setting's data (from previous source or data) to I<source>. Sets the
setting's source to I<source> and empties its data. Returns 1 on success or
undef on error.

=cut

sub writeSource {
    my $self = shift;
    my $insource = shift;
    $self->{error} = "";
    my $data = $self->data();
    return undef if $self->{error};
    unless (defined $data) {
        $self->{error} = "Can't get data for ".$self->source()."\n";
        return undef;
    }
    unless (open(FILE, ">$insource")) {
        $self->{error} = "Can't open \"$insource\" for writing: $!";
        return undef;
    }
    print FILE $data;
    close(FILE);

    $self->{source} = $insource;
    delete $self->{data};
    return 1;
}

=item B<data(I<data>)>

If I<data> is defined, changes the setting's data and empty the source. Returns
the previous data, i.e. if source is defined, read it, if not then the previous
data. Returns undef on error.

=cut

sub data {
    my $self = shift;
    my $indata = shift;
    my $outdata;
    if ($self->{source}) {
        $outdata = $self->readSource();
        return undef if $self->{error};
    } else {
        $outdata = $self->{data};
    }
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
            $self->{error} = "Can't open source for reading \"$self->{source}\": $!";
            return undef;
        }
        $out = join "", <FILE>;
        close(FILE);
    }
    return $out;
}

=item B<useData()>

Fills this setting's data from its source and remove the source (doesn't delete
the file). If doesn't have source, but has data, does nothing. Returns 1 on
success or 0 on failure (failed open source).

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
    $self->{$key} = $value if defined $value;
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

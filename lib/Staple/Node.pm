package Staple::Node;

#
# Copyright (C) 2007-2011 Hebrew University Of Jerusalem, Israel
# See the LICENSE file.
#
# Author: Yair Yarom <irush@cs.huji.ac.il>
#

=head1 NAME

  Staple::Node - Base class for a single node (group, host, distribution, configuration)

=head1 DESCRIPTION

Staple::Node abstract class.

=head1 METHODS

=over

=cut

use strict;
use warnings;

our $VERSION = '006snap';

=item B<type()>

Get this node's type. should be overridden by subclasses.

=cut

sub type {
    return "node";
}

# =item B<note(I<note>)>
# 
# Sets this node's note (string), and returns the previous note. If I<note> is
# undef don't change it, just return the current one.
# 
# =cut
# 
# sub note {
#     return param(shift, "note", shift);
# }

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

Copyright (C) 2007-2011 Hebrew University Of Jerusalem, Israel
See the LICENSE file.

=cut

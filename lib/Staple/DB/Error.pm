package Staple::DB::Error;

#
# Copyright (C) 2007-2010 Hebrew University Of Jerusalem, Israel
# See the LICENSE file.
#
# Author: Yair Yarom <irush@cs.huji.ac.il>
#

use strict;
use warnings;
require Exporter;
use Staple::DB;

our @ISA = ("Staple::DB");
our $VERSION = '006snap';

=head1 NAME

  Staple::DB::Error - Returned when database instance creation failed

=cut

################################################################################
#   Exported
################################################################################

=head1 DESCRIPTION

=over

=item B<new(error message)>

Creates a new instance, with the given error message.

=cut

sub new {
    my $proto = shift;
    my $error = shift;
    $error = "Unknown error" unless $error;
    my $class = ref($proto) || $proto;
    my $self = {};
    $self->{error} = $error;
    bless ($self, $class);
    return $self;
}

=item B<create(error message)>

Creates a new instance with the given error.

=cut

sub create {
    return new($@);
}

sub describe {
    return ("An error. Returned when can't create a real database",
            "Doesn't implement anything. Receives a single string which is the error message"); 
}

sub info {
    my $self = shift;
    return "Error: $self->{error}";
}

################################################################################
#   The end
################################################################################

1;

__END__

=back

=head1 AUTHOR

Yair Yarom, E<lt>irush@cs.huji.ac.ilE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2007-2010 Hebrew University Of Jerusalem, Israel
See the LICENSE file.

=cut

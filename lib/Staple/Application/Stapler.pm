package Staple::Application::Stapler;

#
# Copyright (C) 2007-2011 Hebrew University Of Jerusalem, Israel
# See the LICENSE file.
#
# Author: Yair Yarom <irush@cs.huji.ac.il>
#

use strict;
use warnings;
require Exporter;


use Staple::Application;
use Term::ANSIColor qw(:constants color);
our @ISA = ("Staple::Application");
our $VERSION = '0.2.x';







=head1 NAME

  Staple::Application::Stapler - Used by the stapler program

=cut

################################################################################
#   Methods
################################################################################

=head1 DESCRIPTION

=over

=item B<new([host], [distribution], [database])>

Creates a new instance, set to the given host, distribution (if
present) and database string (if present).

=cut

sub new {
    my $proto = shift;
    my $host = shift;
    my $distribution = shift;
    my $database = shift;
    #return undef unless $host and $distribution;
    my $class = ref($proto) || $proto;
    my $self = {};
    bless ($self, $class);

    $self->clearAll($host, $distribution, $database);

    return $self;
}


# ignore verbose mode, use ours. not disabled either. And keep the tmpDir
# XXX change also the tokens themselves
sub updateData {
    my $self = shift;
    my $verbose = $self->{verbose};
    my $tmpDir = $self->{tmpDir};
    $self->SUPER::updateData();
    $self->{verbose} = $verbose;
    $self->{disabled} = 0;
    $self->setTmpDir($tmpDir);
}

# input: error string
# no output
# prints "Staple: error\n" into stderr
sub error {
    my $self = shift;
    my $error = shift;
    print STDERR RED "Stapler: ";
    print STDERR RESET "$error\n";
}

# input: output string, output level = 0
# no output
# prints "Staple: output\n" into stdout if level <= verbose
sub output {
    my $self = shift;
    my $output = shift;
    my $level = shift;
    $level = 0 unless $level;
    if ($level <= $self->{verbose}) {
        print STDOUT BLUE BOLD "Stapler: ";
        print STDOUT RESET "$output\n";
    }
}

sub debug {
    my $self = shift;
    my $action = shift;
    $self->output("imaginary debug mode ($action) ignored");
}

sub doCriticalAction {
    my $self = shift;
    my $action = "unknown";
    if ($self->{critical} eq "bash" or $self->{critical} eq "prompt") {
        $action = $self->{critical};
    } elsif ($self->{critical} =~ m/^(halt|reboot|poweroff)(\.(\d*))?$/) {
        $action = $1;
    }
    $self->error("Critical error. Would have \"$action\". But instead just exit 2");
    exit 2;
}

# ignore
sub addMail {
    my $self = shift;
    my $message = shift;
    return;
}


################################################################################
#   The end
################################################################################

1;

__END__

=back

=head1 SEE ALSO

L<Staple> - Staple main module.

L<Staple::Application> - the application interface

=head1 AUTHOR

Yair Yarom, E<lt>irush@cs.huji.ac.ilE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2007-2011 Hebrew University Of Jerusalem, Israel
See the LICENSE file.

=cut

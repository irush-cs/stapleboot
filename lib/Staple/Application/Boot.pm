package Staple::Application::Boot;

#
# Copyright (C) 2007-2010 Hebrew University Of Jerusalem, Israel
# See the LICENSE file.
#
# Author: Yair Yarom <irush@cs.huji.ac.il>
#

use strict;
use warnings;
require Exporter;

use Mail::Sendmail;
use Staple;
use Staple::Misc;
use Staple::Application;
use Term::ANSIColor qw(:constants color);
use Net::DNS;
our @ISA = ("Staple::Application");
our $VERSION = '006snap';

use constant LINUX_REBOOT_MAGIC1 => 0xFEE1DEAD;
use constant LINUX_REBOOT_MAGIC2 => 0x20112000;
use constant LINUX_REBOOT_CMD_POWER_OFF => 0x4321FEDC;
use constant LINUX_REBOOT_CMD_HALT => 0xCDEF0123;
use constant LINUX_REBOOT_CMD_RESTART => 0x1234567;

=head1 NAME

  Staple::Application::Boot - Used by the stapleboot program

=cut

################################################################################
#   Methods
################################################################################

=head1 DESCRIPTION

=over

=item B<new(debug, [host], [distribution])>

creates a new instance, set to the given host and distribution (if
present). The debug should be a function called if doCriticalAction is set to
debug (if null error is called with "No debug function").

=cut

sub new {
    my $proto = shift;
    my $debug = shift; 
    my $host = shift;
    my $distribution = shift;
    #return undef unless $host and $distribution;
    my $class = ref($proto) || $proto;
    my $self = {};
    bless ($self, $class);

    $self->clearAll($host, $distribution);
    $self->{sentDebugMail} = 0;
    $self->{mailBody} = "";
    $self->{debugFunction} = $debug;
    
    return $self;
}

# sends mail if first time debug
sub updateData {
    my $self = shift;
    $self->SUPER::updateData();
    if ($self->{debug} and !$self->{sentDebugMail}) {
        $self->{sentDebugMail} = not $self->mail("booting in debug mode", "Warnings: $self->{host} is in debug mode: $self->{debug}");
    }
}

=item B<mail(I<subject, body>)>

Sends mail to $self->{mailto}, 
Returns 0 on success, error message on failure.

=cut

sub mail {
    my $self = shift;
    (my $subject, my $body) = @_;
    my $host = $self->{host};
    unless ($self->{mailto}) {
        $self->error("No mail recipient, not sending mail...");
        return 1;
    }
    if (my $res = new Net::DNS::Resolver) {
        (my $domain) = $res->searchlist;
        $host = "$host.".$domain;
    }
    $host =~ s/^\.+//;
    my $db = $self->{db}->info();
    my $prefix = "Staple version: $VERSION\n";
    $prefix .= "Staple database: $db\n";
    $prefix .= "Distribution: $self->{distribution} (".$self->{db}->getDistributionVersion($self->{distribution}).")\n";
    $prefix .= "Kernel: ".`uname -smr`;
    $prefix .= "\n";
    $prefix .= "=" x 77;
    $body = $prefix."\n\n".$body;
    my %mail = (
                subject => "$self->{host}: $subject",
                to      => $self->{mailto},
                from    => "stapleboot on $self->{host} <root\@$host>",
                Smtp    => $self->{smtpServer},
                Message => $body,
               );
    unless (sendmail(%mail)) {
        $self->error("Can't send mail to $self->{mailto}");
        $self->error("Reason: $Mail::Sendmail::error");
        $self->error("Please notify the system");
        return $Mail::Sendmail::error;
    }
    return 0;
}

# input: error string
# no output
# prints "Staple: error\n" into stderr
sub error {
    local $SIG{__WARN__} = \&Carp::cluck;
    my $self = shift;
    my $error = shift;
    print STDERR RED "Staple: ";
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
        print STDOUT BLUE BOLD "Staple: ";
        print STDOUT RESET "$output\n";
    }
}

sub debug {
    my $self = shift;
    my $action = shift;
    if ($self->{debugFunction}) {
        $self->{debugFunction}($self, $action);
    } else {
        $self->SUPER::debug($action);
    }
}

sub doCriticalAction {
    my $self = shift;
    $self->addMail("Critical failure\nPerforming critical action: \"$self->{critical}\"... ");
    $self->mail("boot errors summary (critical)", $self->{mailBody}) if $self->{mailBody};
    $self->error("Critical error");
    if ($self->{critical} eq "ignore") {
        $self->error("Ignoring critical error...");
    } elsif ($self->{critical} eq "bash" or $self->{critical} eq "prompt") {
        $self->debug($self->{critical});
    } elsif ($self->{critical} =~ m/^(halt|reboot|poweroff)(\.(\d*))?$/) {
        my $action = $1;
        my $wait = $3;
        $wait = 0 unless $wait;
        $self->error("Sleeping $wait seconds before $action");
        $| = 1;
        system("sync &");
        for (my $i = 0; $i < $wait - 1; $i++) {
            sleep 1;
            print ".";
        }
        print "\n";
        $self->output("syncing");
        system("sync &");
        sleep 1;
        $! = 0;
        unless (exec($self->{$action."Command"})) {
            $self->error("Can't die: $!");
            $self->output("Why won't you let me die? :-(");
            exit(3);
        }
    }
    # don't send same mail twice...
    $self->{mailBody} = "";
    $self->{exitCode} = 4;
}

# input: (boot), message
# output: none
# adds the message to $self->{mailBody}
sub addMail {
    my $self = shift;
    my $message = shift;
    if ($self->{mailBody}) {
        $self->{mailBody} .= "=" x 77;
        $self->{mailBody} .= "\n\n";
    }
    $self->{mailBody} .= "$message\n";
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

Copyright (C) 2007-2010 Hebrew University Of Jerusalem, Israel
See the LICENSE file.

=cut

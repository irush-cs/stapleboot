package Staple;

#
# Copyright (C) 2007-2011 Hebrew University Of Jerusalem, Israel
# See the LICENSE file.
#
# Author: Yair Yarom <irush@cs.huji.ac.il>
#

use strict;
use warnings;
require Exporter;
use Staple::Misc;
use Staple::DB::FS;
use Staple::DB::SQL;

=head1 NAME

Staple - Staple main module

=head1 SYNOPSIS

use Staple;

If Staple is not on the standard perl library, then the PERL5LIB should contain
`cat /etc/staple/staple_dir`/lib

=head1 DATA TYPES

B<Staple::Group>

B<Staple::Host>

B<Staple::Distribution>

B<Staple::Configuration>

B<Staple::Mount>

B<Staple::Template>

B<Staple::Script>

B<Staple::Autogroup>

B<token hash>

=over

=over

=item I<key>    - The token name

=item I<value>  - The token value

=item I<type>   - The token type (static, regexp, dynamic)

=item I<raw>    - The raw value: for static, auto and default - the same as I<value>, for regexp and dynmaic, the raw value.

=item I<source> - The origin of the token as free text. Isn't used by staple itself. Used for debugging. (auto, file, group, configuration, host, default, manual, etc.)

=back

=back

=head1 EXPORT

=over

=item setDefaultTokens(tokens ref, default tokens ref)

=for comment =item getDistributionList(distribution)

=item getCompleteMounts(mount list ref, tokens hash ref)

=back

=cut

our @ISA = qw(Exporter);
our @EXPORT_OK = qw();
our @EXPORT = qw(
                    setDefaultTokens
                    getCompleteMounts
               );
our $VERSION = '007snap';

# don't use this, it's just for initializing %defaultTokens
# SMTP_SERVER isn't localhost, as this host most likely doesn't have a mail server running...
my %DEFAULT_TOKENS = (
                      "__STAPLE_SYSINIT__"           => "/etc/init.d/rcS",
                      "__STAPLE_VERBOSE__"           => "1",
                      "__STAPLE_DISABLE__"           => "0",
                      "__STAPLE_DEBUG__"             => "0",
                      "__STAPLE_CRITICAL__"          => "reboot.600",
                      "__STAPLE_MAILTO__"            => "root\@localhost",
                      "__STAPLE_SMTP_SERVER__"       => "smtp",
                      "__STAPLE_BASH__"              => "PS1='(STAPLE)\\u@\\h:\\w\\\$ ' /bin/bash --norc",
                      "__STAPLE_INIT_DISK__"         => "0",
                      "__STAPLE_FIND_LABEL__"        => "",
                      "__STAPLE_LOG__"               => "/var/log/staple",
                      "__STAPLE_MOUNT__"             => "mount -n",
                      "__STAPLE_FSCK_CMD__"          => "/sbin/fsck -a",
                      "__STAPLE_SYSLOG__"            => "LOG_LOCAL6",
                      "__STAPLE_CONF__"              => "/etc/staple/staple.conf",
                      "__STAPLE_CRITICAL_halt__"     => "/sbin/halt -f",
                      "__STAPLE_CRITICAL_reboot__"   => "/sbin/reboot -f",
                      "__STAPLE_CRITICAL_poweroff__" => "/sbin/poweroff -f",
                      "__STAPLE_FSCK_EXIT_OK__"      => "0,1",
                      "__STAPLE_USE_DEFAULT_HOST__"  => "0",
                     );


our %defaultTokens = ();

# why doensn't work from within BEGIN?
foreach my $key (keys %DEFAULT_TOKENS) {
    $defaultTokens{$key} = {key => $key, value => $DEFAULT_TOKENS{$key}, raw => $DEFAULT_TOKENS{$key}, type => "default"};
}


my %allowedTokensValues = (
                           "__STAPLE_VERBOSE__" => qr/^\d+$/,
                           "__STAPLE_DEBUG__" => qr/^(0|bash|prompt)$/,
                           "__STAPLE_CRITICAL__" => qr/^(0|ignore|bash|prompt|halt(\.\d+)?|reboot(\.\d+)?|poweroff(\.\d+)?)$/,
                           "__STAPLE_SYSLOG__" => qr/^(LOG_AUTHPRIV|LOG_CRON|LOG_DAEMON|LOG_FTP|LOG_KERN|LOG_LOCAL[0-7]|LOG_LPR|LOG_MAIL|LOG_NEWS|LOG_SYSLOG|LOG_USER|LOG_UUCP|)$/,
                          );

my %mountTokenToOption = (
                          "SOURCE"       => "source",
                          "TYPE"         => "type",
                          "OPTIONS"      => "options",
                          "NEXT"         => "next",
                          "PERMISSIONS"  => "permissions",
                          "CRITICAL"     => "critical",
                          "COPY_SOURCE"  => "copySource",
                          "COPY_FILES"   => "copyFiles",
                          "COPY_EXCLUDE" => "copyExclude",
                          "COPY_LINKS"   => "copyLinks",
                          "MANUAL"       => "manual",
                          "FSCK"         => "fsck",
                          "FSCK_CMD"     => "fsckCommand",
                          "FSCK_EXIT_OK" => "fsckExitOK",
                         );

my %mountTokenDefaultValues = (
                               "SOURCE"       => "",
                               "TYPE"         => "",
                               "OPTIONS"      => "",
                               "NEXT"         => "",
                               "PERMISSIONS"  => "",
                               "CRITICAL"     => "",
                               "COPY_SOURCE"  => "",
                               "COPY_FILES"   => "",
                               "COPY_EXCLUDE" => "",
                               "COPY_LINKS"   => "",
                               "MANUAL"       => "",
                               "FSCK"         => "1",
                               "FSCK_CMD"     => "",
                               "FSCK_EXIT_OK" => "",
                              );


################################################################################
#   Exported
################################################################################


=head1 DESCRIPTION

# =item B<getDistributionList(I<distribution>)>
# 
# Retruns an ordered list of the distribution branch starting with the given I<distribution>.
# 
# =cut
#
#sub getDistributionList {
#    my $dist = shift;
#    my $distDir = "$stapleDir/distributions/$dist";
#    my @distributions = ();
#    if (-d "$distDir") {
#        push @distributions, $dist;
#        if (-l "$distDir/previous") {
#            my $previous = readlink "$distDir/previous";
#            $previous =~ s!^../!!;
#            push @distributions, getDistributionList($previous);
#        }
#    }
#    return @distributions;
#}

=item B<getCompleteMounts(I<mount list ref, tokens hash ref>)>

Returns an ordered list (same order), of full, active mounts, one mount per
destination.

=cut

sub getCompleteMounts {
    my @mounts = @{$_[0]};
    my %tokens = %{$_[1]};
    @mounts = cleanMounts(@mounts);
    @mounts = buildMounts(\@mounts, \%tokens);

    return @mounts;   
}

=back

=cut
    
################################################################################
#   Autos Internals
################################################################################

################################################################################
#   Scripts Internals
################################################################################

################################################################################
#   Mounts Internals
################################################################################
   
# input: list of ordered mount hashes
# output: list of ordered active uniq mounts hashes
sub cleanMounts {
    my @raw = @_;
    my %mounts = ();
    foreach my $mount (@raw) {
        if ($mount->active()) {
            $mounts{$mount->destination()} = $mount;
        } else {
            delete $mounts{$mount->destination()}
        }
    }
    my @output = ();
    my %uniq = ();
    foreach my $mount (@raw) {
        if ($mounts{$mount->destination()} and ! $uniq{$mount->destination()}) {
            push @output, $mount;
            $uniq{$mount->destination()} = 1;
        }
    }
    return @output;
}

# input: list ref of mount hashes, hash ref of tokens
# output: list of full mount hashes (fixed paths)
sub buildMounts {
    my $oldMounts = $_[0];
    my $tokens = $_[1];
    my @mounts = ();
    foreach my $oldMount (@$oldMounts) {
        my %mount = %$oldMount;
        for my $option (keys %mountTokenToOption) {
            my $key = "__MOUNT_$mount{destination}_${option}__";
            if ($tokens->{$key}) {
                $mount{$mountTokenToOption{$option}} = $tokens->{$key}->{value}
            } else {
                $mount{$mountTokenToOption{$option}} = $mountTokenDefaultValues{$option};
            }
        }
        $mount{manual} = 1 if $mount{critical};
        $mount{manual} = 1 if $mount{next};
        push @mounts, Staple::Mount->new(\%mount);
    }
    return @mounts;
}


################################################################################
#   The end
################################################################################

1;
__END__

=head1 EXAMPLES

The init stage from the stapleboot (obsolete...):

=for comment @distributions = getDistributionList($distribution);

=over

 @groups = getCompleteGroups(getDistributionGroup($distribution), getHostGroup($host));
 @configurations = getCompleteConfigurations([getGroupsConfigurations(@groups)], $distributions);
 //%tokens = getCompleteTokens(getRawTokens(@configurations, @groups), $host, $distribution);
 @mounts = getCompleteMounts([getRawMounts(@configurations)], \%tokens);
 @templates = getTemplates(@configurations);
 @scripts = getScripts(@configurations);
 @autos = getAutos(@configurations);

=back

=head1 SEE ALSO

L<Staple::Misc> - Staple miscellaneous utilities.

L<Staple::DB> - Staple database connection interface

=head1 AUTHOR

Yair Yarom, E<lt>irush@cs.huji.ac.ilE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2007-2011 Hebrew University Of Jerusalem, Israel
See the LICENSE file.

=cut

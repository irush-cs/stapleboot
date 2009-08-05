package Staple;

#
# Copyright (C) 2007-2009 Hebrew University Of Jerusalem, Israel
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

B<group hash>

=over

=over

=item I<name>   - The name of the group

=item I<type>   - The type of the group (group, distribution, host, or auto)

=item I<path>   - The path of the group in the filesystem

=for comment =item I<active> - Whether this group is active (1) or not (0)

=back

=back

B<configuration hash>

=for comment
Don't add "type" to configuration as some distinguish between group and configuration by the availability of the "type" key

=over

=over

=item I<name>   - The name of the configuration

=item I<path>   - The full path of the configuration (filesystem)

=item I<dist>   - The distribution

=item I<active> - Whether the configuration is active (1) or not (0)

=item I<group>  - Originating group hash (if any)

=back

=back

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

B<mount hash>

=over

=over

=item I<source>         - The device

=item I<destination>    - The mounting point

=item I<type>           - Mount type (eg. tmpfs, bind, etc.)

=item I<options>        - Mount options 

=item I<active>         - Whether the mount is active (1) or not (0)

=item I<permissions>    - Mount permissions (octal)

=item I<next>           - Next mount if this one fails (a configuration name), only works on manual mode

=item I<critical>       - Whether to declare a critical state if fails (implies manual).

=item I<configuration>  - Configuration for this mount

=item I<copySource>     - The location of the source files to copy from (no copy if empty)

=item I<copyFiles>      - The files to copy from the source (defaults to .)

=item I<copyExclude>    - The fiels to exclude from the source (default to "")

=item I<manual>         - Whether to manually mount (1) or just write an fstab entry (0)

=item I<fsck>           - Whether to run fsck (1) or not (0).

=item I<fsckCommand>    - Special fsck command for this mount (defaults to empty, only for manual mount)

=back

=back

B<Template hash>

=over

=over

=item I<source>        - The source of the template (i.e. the full pathed location in the filesystem)

=item I<data>          - If (and only if) I<source> is empty, contains the actual data of the template.

=item I<destination>   - The destination path

=item I<stage>         - The stage this template should be copied (mount, sysinit, or final)

=item I<configuration> - The configuration of this template (hash ref)

=item I<mode>          - The mode (octal) for the template

=item I<gid>           - The gid for the template

=item I<uid>           - The uid for the template

=back

=back

B<Script hash>

=over

=over

=item I<name>          - The name of the script

=item I<source>        - The source of the script (i.e. the full pathed location in the filesystem)

=item I<data>          - If (and only if) I<source> is empty, contains the actual data of the script.

=item I<configuration> - The configuration of this script (hash ref)

=item I<stage>         - The stage this script should run in (auto, mount, sysinit, or final)

=item I<order>         - The location of this script in the local order (a number)

=item I<critical>      - Whether this script should invoke critical action when it fails (0 or 1)

=item I<tokens>        - Whether this script should pass through tokens substitution before running (0 or 1)

=item I<tokenScript>   - Whether this script is a token script, i.e. the output will change the tokens (0 or 1)

=back

=back

B<Auto hash>

=over

=over

=item I<name>          - The name of the auto

=item I<source>        - The source of the auto (i.e. the full pathed location of the script)

=item I<data>          - If (and only if) I<source> is empty, contains the actual data of the script.

=item I<configuration> - The configuration of this auto (hash ref)

=item I<order>         - The location of this auto in the local order (a number)

=item I<critical>      - Whether this auto should invoke critical action when it fails (0 or 1)

=item I<tokens>        - Whether this auto should pass through tokens substitution before running (0 or 1)

=back

=back

=head1 EXPORT

=over

=item getCompleteTokens(tokens ref [host] [distribution])

=item setDefaultTokens(tokens ref, default tokens ref)

=item getStapleDir( )

=for comment =item getDistributionList(distribution)

=item getConfigurationsByName(configuration [configuration [...]])

=item getCompleteMounts(mount list ref, tokens hash ref)

=back

=cut

our @ISA = qw(Exporter);
our @EXPORT_OK = qw();
our @EXPORT = qw(
                    getCompleteTokens
                    setDefaultTokens
                    getStapleDir
                    getConfigurationsByName
                    getCompleteMounts
               );
our $VERSION = '003';

my $stapleDir;

# don't use this, it's just for initializing %defaultTokens
# SMTP_SERVER isn't localhost, as this host most likely doesn't have a mail server running...
my %DEFAULT_TOKENS = (
                      "__STAPLE_SYSINIT__"     => "/etc/init.d/rcS",
                      "__STAPLE_VERBOSE__"     => "1",
                      "__STAPLE_DISABLE__"     => "0",
                      "__STAPLE_DEBUG__"       => "0",
                      "__STAPLE_CRITICAL__"    => "reboot.600",
                      "__STAPLE_MAILTO__"      => "root\@localhost",
                      "__STAPLE_SMTP_SERVER__" => "smtp",
                      "__STAPLE_BASH__"        => "PS1='(STAPLE)\\u@\\h:\\w\\\$ ' /bin/bash --norc",
                      "__STAPLE_INIT_DISK__"   => "0",
                      "__STAPLE_FIND_LABEL__"  => "",
                      "__STAPLE_LOG__"         => "/var/log/staple",
                      "__STAPLE_MOUNT__"       => "mount -n",
                      "__STAPLE_FSCK_CMD__"    => "/sbin/fsck -a",
                      "__STAPLE_SYSLOG__"      => "LOG_LOCAL6",
                      "__STAPLE_CONF__"        => "/etc/staple/staple.conf",
                     );


our %defaultTokens = ();

# why doensn't work from within BEGIN?
foreach my $key (keys %DEFAULT_TOKENS) {
    $defaultTokens{$key} = {key => $key, value => $DEFAULT_TOKENS{$key}, raw => $DEFAULT_TOKENS{$key}, type => "default"};
}


my %allowedTokensValues = (
                           "__STAPLE_VERBOSE__" => qr/^\d+$/,
                           "__STAPLE_DEBUG__" => qr/^(0|bash|prompt)$/,
                           "__STAPLE_CRITICAL__" => qr/^(0|bash|prompt|halt(\.\d+)?|reboot(\.\d+)?|poweroff(\.\d+)?)$/,
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
                              );

BEGIN {
    #$error = "";
    
    #if (-r "/etc/staple/staple_dir") {
    #    open(FILE, "/etc/staple/staple_dir");
    #    my $dir = <FILE>;
    #    close(FILE);
    #    chomp $dir;
    #    $stapleDir = $dir;
    #} else {
    #    #print STDERR "warning: can't get staple directory (from /etc/staple/staple_dir), assuming /staple\n";
    #    $error = "Can't get staple directory from /etc/staple/staple_dir, assuming /staple";
    #    $stapleDir = "/staple";
    #}

    $stapleDir = "/boot/staple";
    #die "/boot/staple doesn't exists, can't find staple database" unless -d $stapleDir;
    
    #    $db = Staple::DB::FS->new($stapleDir);
    #    die "Can't open filesystem database" unless defined $db;
}

################################################################################
#   Exported
################################################################################


=head1 DESCRIPTION

=over

=item B<getCompleteTokens(I<tokens ref [host] [distribution]>)>

Receives a tokens hash (as B<DB::getTokens> outputs) and returns a tokens hash,
with simple value checking (e.g. for __STAPLE_*__ tokens), auto and default
tokens (auto tokens that were wrongly inserted, will be removed), and dynamic
and regexp tokens are evaluated (in that order).

The optional I<host> and I<distribution> parameters are for the
__AUTO_HOSTNAME__ and __AUTO_DISTRIBUTION__ tokens. If host is omitted, this
token will not be included in the results

=cut

sub getCompleteTokens {
    
    my %tokens = %{$_[0]};
    my $host;
    my $distribution;
    $host = $_[1] if $_[1];
    $distribution = $_[2] if $_[2];
    
    my @delete = ();
    foreach my $key (keys %tokens) {
        push @delete, $key if $key =~ m/^__AUTO_/;
    }
    delete @tokens{@delete};
    $tokens{__AUTO_DISTRIBUTION__} = {key => "__AUTO_DISTRIBUTION__", value => $distribution, raw => $distribution, type => "static", source => "auto"} if $distribution;
    $tokens{__AUTO_HOSTNAME__} = {key => "__AUTO_HOSTNAME__", value => $host, raw => $host, type => "static", source => "auto"} if $host;
    $tokens{__AUTO_TMP__} = {key => "__AUTO_TMP__", value => "$stapleDir/tmp", raw => "$stapleDir/tmp", type => "static", source => "auto"};
    #$tokens{__AUTO_IP__} = $ip;
    %tokens = setDefaultTokens(\%tokens, \%defaultTokens);
    %tokens = verifyTokens(\%tokens, \%allowedTokensValues);
    for (my $i = 0; $i < 2; $i++) {
        %tokens = setDynamicTokens(%tokens);
        %tokens = setRegexpTokens(%tokens);
    }
    #setVariablesFromTokens(\%tokens, \%tokensToVariables);
    return %tokens;
}


=item B<setDefaultTokens(I<tokens ref>, I<default tokens ref>)>

Receives a tokens hash (as B<DB::getTokens> outputs), and a default tokens hash,
and returns a tokens hash. The results hash is the original hash (a copy), with
the defaults if not set. if __STAPLE_CONF__ is set to a readable file (either
by the tokens, or by the default tokens), than first the file is read and
applied (not recursivally). The tokens read from file are ignored if not valid.

=cut


# input: tokens hash ref, default hash ref
# output: token hashes list (with defaults, if undefined)
sub setDefaultTokens {
    my %tokens = %{$_[0]};
    my %defaults = %{$_[1]};
    my $conf = "";
    $conf = $tokens{__STAPLE_CONF__}->{value} if $tokens{__STAPLE_CONF__};
    $conf = $defaults{__STAPLE_CONF__}->{value} if $defaults{__STAPLE_CONF__} and (not $conf or not -r $conf);
    if (-r $conf) {
        my %files = readTokensFile($conf, "static");
        map {exists $_->{source} and $_->{source} = "default:$_->{source}" or $_->{source} = "default"} values %files;
        %files = verifyTokens(\%files, \%allowedTokensValues);
        foreach my $key (keys %files) {
            $tokens{$key} = $files{$key} unless exists $tokens{$key};
        }
    }

    foreach my $key (keys %defaults) {
        $tokens{$key} = {key => $key, value => $defaults{$key}->{value}, raw => $defaults{$key}->{raw}, type => "static", source => "default"} unless exists $tokens{$key};
    }
    return %tokens;
}


#=item B<getDistributionList(I<distribution>)>
#
#Retruns an ordered list of the distribution branch starting with the given I<distribution>.
#
#=cut
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

=item B<getConfigurationsByName(I<configuration [configuration [...]]>)>

Receives a list of configurations names (strings), and returns a list an
incomplete (missing path, distribution, and group), active configuration hash
refs (order preserved).

=cut

sub getConfigurationsByName {
    my @configurations = ();
    while (my $configuration = shift) {
        push @configurations, {name => $configuration, path => undef, dist => undef, active => 1, group => undef};
    }
    return @configurations;
}

=item B<getStapleDir( )>

Returns the staple directory as writen in F</etc/staple/staple_dir>. Returns C</staple> if doesn't exists.

=cut

sub getStapleDir {
    return $stapleDir;
}

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
        if ($mount->{active}) {
            $mounts{$mount->{destination}} = $mount;
        } else {
            delete $mounts{$mount->{destination}}
        }
    }
    my @output = ();
    my %uniq = ();
    foreach my $mount (@raw) {
        if ($mounts{$mount->{destination}} and ! $uniq{$mount->{destination}}) {
            push @output, $mount;
            $uniq{$mount->{destination}} = 1;
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
        push @mounts, \%mount;
    }
    return @mounts;
}


################################################################################
#   Tokens Internals
################################################################################

# input: tokens hash ref, allowed hash ref
# output: tokens hash list (without bad tokens)
sub verifyTokens {
    my %tokens = %{$_[0]};
    my %allowed = %{$_[1]};
    foreach my $key (keys %allowed) {
        if ($tokens{$key}) {
            delete $tokens{$key} unless $tokens{$key}->{value} =~ m/^$allowed{$key}$/;
        }
    }
    return %tokens;
}

# input: tokens hash
# output: same hashes with dynamic evaluated
sub setDynamicTokens {
    my %tokens = @_;
    foreach my $tokenName (grep {$tokens{$_}->{type} eq "dynamic"} keys %tokens) {
        my $token = $tokens{$tokenName};
        my %currentTokens = %tokens;
        delete $currentTokens{$tokenName};
        my $value;
        $token->{value} = $token->{raw};
        do {
            $value = $token->{value};
            $token->{value} = applyTokens($value, \%currentTokens);
        } while ($token->{value} ne $value);
    }
    return %tokens;
}

# input: tokens hash
# output: same hash with regexp evaluated
sub setRegexpTokens {
    my %tokens = @_;
    foreach my $token (sort {$a cmp $b} grep {$tokens{$_}->{type} eq "regexp"} keys %tokens) {
        $tokens{$token}->{value} = join $tokens{$token}->{raw}, map {$tokens{$_}->{value}} sort {$a cmp $b} grep {/^$token$/ and $tokens{$_}->{type} ne "regexp"} keys %tokens;
    }
    return %tokens;
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
 %tokens = getCompleteTokens(getRawTokens(@configurations, @groups), $host, $distribution);
 @mounts = getCompleteMounts([getRawMounts(@configurations)], \%tokens);
 @templates = getTemplates(@configurations);
 @scripts = getScripts(@configurations);
 @autos = getAutos(@configurations);

=back

=head1 SEE ALSO

L<Staple::Misc> - Staple miscellaneous utilities.

L<Staple::DB> - Staple database connection interface

L<Staple::Sync> - Syncs between to databases

=head1 AUTHOR

Yair Yarom, E<lt>irush@cs.huji.ac.ilE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2007-2009 Hebrew University Of Jerusalem, Israel
See the LICENSE file.

=cut

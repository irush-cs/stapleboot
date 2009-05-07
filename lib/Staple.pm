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

=item I<source> - The origin of the token as free text. Isn't used by staple itself. Used for debugging. (auto, file, group, configuration, host, default, etc.)

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

=item setDB(E<lt>"fs"|"sql"E<gt> [params ...])

=item getDB

=item addHost(host)

=item removeHost(host)

=item addGroup(group name)

=item removeGroup(group name)

=item addDistribution(distribution)

=item removeDistribution(distribution)

=item addConfiguration(distribution, configuration)

=item removeConfiguration(distribution, configuration)

=item copyConfiguration(configuration name, from distribution, to distribution)

=item addTokens(tokens hash ref, group|configuration ref)

=item removeTokens(tokens names list ref, group|configuration ref)

=item addMount(configuration, mount, [location])

=item removeMounts(mount [mount [...]])

=item getRawTokens(group|configuration [group|configuration [...]])

=item getCompleteTokens(tokens ref [host] [distribution])

=item setDefaultTokens(tokens ref, default tokens ref)

=item getStapleDir( )

=item getLastError( )

=item getAllHosts( )

=item getAllGroups( )

=item getAllDistributions( )

=item getAllConfigurations(distribution)

=item getDistributionGroup(distribution)

=item getHostGroup(host)

=item getCompleteGroups(group [group [...]])

=item getGroupGroups(group [group [...]])

=item addGroupGroup(group to receive, group name to add, location)

=item removeGroupGroups(group, group name list)

=item addGroupConfiguration(group, configuration, location)

=item getGroupsConfigurations(group [group [...]])

=item removeGroupConfigurations(group, configuration [configuration [...]])

=item getCompleteConfigurations(configurations ref, distribution, [bad list ref])

=for comment =item getDistributionList(distribution)

=item getConfigurationsByName(configuration [configuration [...]])

=item getGroupsByName(group name [group name [...]])

=item getRawMounts(configuration [configuration [...]])

=item getCompleteMounts(mount list ref, tokens hash ref)

=item getTemplates(configuration [configuration [...]])

=item addTemplates(template [template [...]]);

=item removeTemplates(template [template [...]]);

=item addScripts(script [script [...]]);

=item removeScripts(script [script [...]]);

=item getScripts(configuration [configuration [...]])

=item getAutos(configuration [configuration [...]])

=item addAutos(auto [auto [...]])

=item removeAutos(auto [auto [...]])

=item whoHasGroup(group name) 

=item whoHasConfiguration(configuration name) 

=item whoHasToken(token key, distribution)

=back

=cut

our @ISA = qw(Exporter);
our @EXPORT_OK = qw();
our @EXPORT = qw(
                    setDB
                    getDB
                    getRawTokens
                    getCompleteTokens
                    setDefaultTokens
                    addHost
                    removeHost
                    addGroup
                    removeGroup
                    addDistribution
                    removeDistribution
                    addConfiguration
                    removeConfiguration
                    copyConfiguration
                    addTokens
                    removeTokens
                    addMount
                    removeMounts
                    getStapleDir
                    getLastError
                    getAllHosts
                    getAllGroups
                    getAllDistributions
                    getAllConfigurations
                    getDistributionGroup
                    getHostGroup
                    getCompleteGroups
                    getGroupGroups
                    addGroupGroup
                    removeGroupGroups
                    addGroupConfiguration
                    removeGroupConfigurations
                    getGroupsConfigurations
                    getCompleteConfigurations
                    getConfigurationsByName
                    getGroupsByName
                    getRawMounts
                    getCompleteMounts
                    getTemplates
                    addTemplates
                    removeTemplates
                    addScripts
                    removeScripts
                    getScripts
                    getAutos
                    addAutos
                    removeAutos
                    whoHasGroup
                    whoHasConfiguration
                    whoHasToken
               );
our $VERSION = '002';

my $stapleDir;
my $error;
my $db;

# don't use this, it's just for initializing %defaultTokens
# SMTP_SERVER isn't localhost, as this host most likely doesn't have an mail server running...
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
                          "SOURCE"  => "source",
                          "TYPE"    => "type",
                          "OPTIONS" => "options",
                          "NEXT"    => "next",
                          "PERMISSIONS" => "permissions",
                          "CRITICAL" => "critical",
                          "COPY_SOURCE" => "copySource",
                          "COPY_FILES" => "copyFiles",
                          "COPY_EXCLUDE" => "copyExclude",
                          "COPY_LINKS" => "copyLinks",
                          "MANUAL" => "manual",
                         );

BEGIN {
    $error = "";
    
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
    
    $db = Staple::DB::FS->new($stapleDir);
    die "Can't open filesystem database" unless defined $db;
}

################################################################################
#   Exported
################################################################################


=head1 DESCRIPTION

=over

=item B<setDB(I<E<lt>"fs"|"sql"E<gt> [params ...] >)>

Set the database to connect to for the remainder of the session. Returns 1 on
success, adn 0 on failure (and sets the error).

=over 

=item * fs

Use the filesystem as database (the default), with this database, there are no
comments. The default is with /etc/staple/staple_dir. may override with 1
parameter. This is the default database

=item * sql

Use an (postgre)sql database. The first parameter is the schema to use, the
default is "staple". If an empty string is given, no schema assumed.  The
second parameter is database connection parameters, the default is
"dbi:Pg:dbname=staple;host=pghost;port=5432;".

=back

=cut

sub setDB {
    my $type = shift;
    $type = "fs" unless $type;
    my @params = @_;
    my $newStapleDir;
    my $newDb;
    if ($type =~ m/fs/i) {
        $newStapleDir = $params[0] if $params[0];
        $newDb = Staple::DB::FS->new($newStapleDir);
        if ($newDb) {
            $db = $newDb;
            $stapleDir = $newStapleDir;
            return 1;
        }
        $error = "can't open filesystem databse";
        return 0;
    } elsif ($type =~ m/sql/i) {
        $params[0] = "$params[0]" if defined $params[0];
        $params[0] = "staple" unless defined $params[0];
        $params[1] = "dbi:Pg:dbname=staple;host=pghost;port=5432;" unless $params[1];
        $newDb = Staple::DB::SQL->new(@params);
        if ($newDb) {
            $db = $newDb;
            return 1;
        }
        $error = "can't open sql database";
        return 0;
    }
    $error = "Unknown database $type";
    return 0;
}


=item B<getDB>

Returns a string representing the current database (as given via setDB)

=cut

sub getDB {
    return $db->info();
}

=item B<addHost(I<host>)>

Adds a host, returns 1 on success, and 0 on failure (and sets the error)

=cut

sub addHost {
    my $host = shift;
    unless ($host) {
        $error = "Missing host name";
        return 0;
    }
    if ($host =~ m\/\) {
        $error = "Host name can't contain '\/'";
        return 0;
    }
    unless ($db->addHost($host)) {
        $error = $db->{error};
        return 0;
    }
    return 1;
}

=item B<removeHost(I<host>)>

Deletes a host, returns 1 on success, and 0 on failure (and sets the error)

=cut

sub removeHost {
    my $host = shift;
    unless ($host) {
        $error = "Missing host name";
        return 0;
    }
    unless ($db->removeHost($host)) {
        $error = $db->{error};
        return 0;
    }
    return 1;
}

=item B<addGroup(I<group string>)>

Adds a group, returns 1 on success, and 0 on failure (and sets the error)

=cut

sub addGroup {
    my $group = shift;
    unless ($group) {
        $error = "Missing group name";
        return 0;
    }
    unless ($db->addGroup($group)) {
        $error = $db->{error};
        return 0;
    }
    return 1;
}

=item B<removeGroup(I<group string>)>

Deletes a group, returns 1 on success, and 0 on failure (and sets the error)

=cut

sub removeGroup {
    my $group = shift;
    unless ($group) {
        $error = "Missing group name";
        return 0;
    }
    unless ($db->removeGroup($group)) {
        $error = $db->{error};
        return 0;
    }
    return 1;
}

=item B<addGroupConfiguration(I<group configuration [location]>)>

Adds the given configuration (should have "name" and "active" fields set) to
the I<group>'s configuration list at I<location>. If location is omitted
appends to the end. Returns 1 on success or undef on failure.

=cut

sub addGroupConfiguration {
    my $group = shift;
    my $configuration = shift;
    my $location = shift;
    unless ($group) {
        $error = "Missing group";
        return undef;
    }
    unless ($configuration) {
        $error = "Missing configuration";
        return undef;
    }
    unless ($db->addGroupConfiguration($group, $configuration, $location)) {
        $error = $db->{error};
        return undef;
    }
    return 1;
}

=item B<addDistribution(I<distribution>)>

Adds a distribution, returns 1 on success, and 0 on failure (and sets the error)

=cut

sub addDistribution {
    my $distribution = shift;
    unless ($distribution) {
        $error = "Missing distribution name";
        return 0;
    }
    unless ($db->addDistribution($distribution)) {
        $error = $db->{error};
        return 0;
    }
    return 1;
}

=item B<removeDistribution(I<distribution string>)>

Deletes a distribution, returns 1 on success, and 0 on failure (and sets the error)

=cut

sub removeDistribution {
    my $distribution = shift;
    unless ($distribution) {
        $error = "Missing distribution name";
        return 0;
    }
    unless ($db->removeDistribution($distribution)) {
        $error = $db->{error};
        return 0;
    }
    return 1;
}

=item B<addConfiguration(I<distribution, configuration string>)>

Adds a configuration, returns 1 on success, and 0 on failure (and sets the error)

=cut

sub addConfiguration {
    my $distribution = shift;
    my $configuration = shift;
    unless ($distribution) {
        $error = "Missing distribution name";
        return 0;
    }
    unless ($configuration) {
        $error = "Missing configuration name";
        return 0;
    }
    unless ($db->addConfiguration($distribution, $configuration)) {
        $error = $db->{error};
        return 0;
    }
    return 1;
}

=item B<addTokens(I<tokens hash ref, group|configuration ref>)>

Add the tokens (hash ref of tokens) to the given group or
configuration. returns 1 on success, and 0 on failure (and sets the error)

=cut

sub addTokens {
    my $tokens = shift;
    my $group = shift;
    unless ($tokens) {
        $error = "Missing tokens";
        return 0;
    }
    unless ($group) {
        $error = "Missing group";
        return 0;
    }
    if (grep {/=/} keys %$tokens) {
        $error = "Tokens may not contain the '=' char";
        return 0;
    }
    unless ($db->addTokens($tokens, $group)) {
        $error = $db->{error};
        return 0;
    }
    return 1;
}

=item B<addMount(I<configuration, mount, [location]>)>

Add the mount (includes active and destination) to the given configuration in
the given location (or the end if omitted). returns 1 on success, or undef on
failure (and sets the error)

=cut

sub addMount {
    my $configuration = shift;
    my $mount = shift;
    my $location = shift;
    unless ($configuration) {
        $error = "Missing configuration";
        return 0;
    }
    unless ($mount) {
        $error = "Missing mount";
        return 0;
    }
    unless ($db->addMount($configuration, $mount, $location)) {
        $error = $db->{error};
        return undef;
    }
    return 1;
}

=item B<removeMounts(I<mount [mount [...]]>)>

Removes the given mounts (list of hashes). The mount hashes should contains a
valid destination, active and configuration. Returns 1 or undef.

=cut

sub removeMounts {
    my @mounts = @_;
    unless (@mounts) {
        $error = "Missing mounts";
        return undef;
    }
    unless ($db->removeMounts(@mounts)) {
        $error = $db->{error};
        return undef;
    }
    return 1;
}

=item B<removeTokens(I<tokens names list ref, group|configuration ref>)>

Removes the tokens (list of strings) from the given group or
configuration. returns 1 on success, and 0 on failure (and sets the error)

=cut

sub removeTokens {
    my $tokens = shift;
    my $group = shift;
    unless ($tokens) {
        $error = "Missing tokens";
        return 0;
    }
    unless ($group) {
        $error = "Missing group";
        return 0;
    }
    unless ($db->removeTokens($tokens, $group)) {
        $error = $db->{error};
        return 0;
    }
    return 1;
}

=item B<removeConfiguration(I<distribution, configuration string>)>

Deletes a configuration, returns 1 on success, and 0 on failure (and sets the error)

=cut

sub removeConfiguration {
    my $distribution = shift;
    my $configuration = shift;
    unless ($distribution) {
        $error = "Missing distribution name";
        return 0;
    }
    unless ($configuration) {
        $error = "Missing configuration name";
        return 0;
    }
    unless ($db->removeConfiguration($distribution, $configuration)) {
        $error = $db->{error};
        return 0;
    }
    return 1;
}

=item copyConfiguration(configuration name, from distribution, to distribution)

copyies the configuration (string), from one distribution to another including
everything below it. (i.e. coping '/' will copy the entire configuration tree).

returns 1 on success, and 0 on failure (and sets the error).

=cut

sub copyConfiguration {
    (my $conf, my $from, my $to) = @_;
    unless ($conf) {
        $error = "Missing configuration name";
        return 0;
    }
    unless ($from) {
        $error = "Missing source distribution";
        return 0;
    }
    unless ($to) {
        $error = "Missing destination distribution";
        return 0;
    }
    unless ($db->copyConfiguration($conf, $from, $to)) {
        $error = $db->{error};
        return 0;
    }
    return 1;
}

=item B<getRawTokens(I<group|configuration [group|configuration [...]]>)>

Returns a tokens hash reference (where the key is the token key, and the value
is the token hash). The tokens are taken from the groups and configurations
(which can be intermixed in the input list), by the same order, so if token
appears twice it will be overridden.

The tokens are returned raw from the database/filesystem, they are not
C<initialized>. i.e. they aren't check for mistakes, no auto and default tokens
are added, and the regexp and dynamic tokens' values are empty (not
evaluated). To C<initialize> them, use the B<getCompleteTokens> function.

If an error occurs, undef is returned, and the last error is set.

=cut

sub getRawTokens {
    my @groupsAndConfigurations = @_;
    my $tokens;
    unless ($tokens = $db->getTokens(@groupsAndConfigurations)) {
        $error = $db->{error};
        return undef
    }
    return $tokens;
}

=item B<getCompleteTokens(I<tokens ref [host] [distribution]>)>

Receives a tokens hash (as B<getRawTokens> outputs) and returns a tokens hash,
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

Receives a tokens hash (as B<getRawTokens> outputs), and a default tokens hash,
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


=item B<getHostGroup(I<host>)>

Returns the group of the host. Returns undef if host doesn't exist and sets the
error.

=cut

sub getHostGroup {
    my $host = shift;
    if (my $group = $db->getHostGroup($host)) {
        return $group;
    }
    $error = $db->{error};
    return undef;
}

=item B<getDistributionGroup(I<distribution>)>

Returns the distribution group hash ref. If the distribution does not exists,
undef is returned.

=cut

sub getDistributionGroup {
    my $distribution = shift;
    if (my $group = $db->getDistributionGroup($distribution)) {
        return $group;
    }
    $error = $db->{error};
    return undef;
}

=item B<getCompleteGroups(I<group [group [...]]>)>

Returns a complete list of groups. Groups that have extra group, are computed
and placed before the given group. Groups will be splitted into intermediate
groups, and duplicate groups will be removed.

WARNING: try to avoid circular groups dependencies 

=cut

sub getCompleteGroups {
    my @rawGroups = @_;
    my @groups = ();
    my %groups = ();

    @rawGroups = fillIntermediate(@rawGroups);
    map {$_->{path} = $db->getGroupPath($_->{name}) if $_->{type} eq "group"} @rawGroups;

    foreach my $rawGroup (@rawGroups) {
        unless ($groups{$rawGroup->{name}}) {
            my @newGroups = getCompleteGroups(getGroupGroups($rawGroup));
            foreach my $newGroup (@newGroups) {
                unless ($groups{$newGroup->{name}}) {
                    $groups{$newGroup->{name}} = 1;
                    push @groups, $newGroup;
                }
            }
            $groups{$rawGroup->{name}} = 1;
            push @groups, $rawGroup;
        }
    }

    return @groups;
}


=item B<getGroupsConfigurations(I<group [group [...]]>)>

Returns an ordered list of configurations (both active and inactive) from the
groups list. The configurations aren't full, i.e. I<path> and I<dist> are
undef. To get complete configurations, as they would appear in the boot
process, pass them through I<getCompleteConfigurations>.  In case of error,
undef will be returned and the last error will be set.

=cut

sub getGroupsConfigurations {
    my @groups = @_;
    my @configurations = ();
    foreach my $group (@groups) {
        my @localConfs = $db->getGroupConfigurations($group);
        if (@localConfs == 1 and not defined $localConfs[0]) {
            $error = $db->{error};
            return undef;
        }
        push @configurations, @localConfs;
    }
    return @configurations;
}


=item B<getGroupGroups(I<group>)>

Returns an ordered list of raw group (no intermediate, no recursive) associated
with the given group. In case of error, undef will be returned and the last
error will be set.

=cut

sub getGroupGroups {
    my $group = shift;
    my @groups = $db->getGroups($group);
    if (@groups and not defined $groups[0]) {
        $error = $db->{error};
        return undef;
    }
    return $db->getGroupsByName(@groups);
}


=item B<addGroupGroup(I<group, group name, location>)>

The first group is the receiver. The second group (name - string), is the group
to add to the first group. The third, optional, parameter is the location in
the gorup list, if omitted adds to the end of the list.

Returns 1 on succes or undef on failure (error is set).

=cut

sub addGroupGroup {
    my $group = shift;
    my $name = shift;
    my $location = shift;
    unless ($group) {
        $error = "Missing group parameter";
        return undef;
    }
    unless ($name) {
        $error = "Missing group to add";
        return undef;
    }
    unless ($db->addGroupGroup($group, $name, $location)) {
        $error = $db->{error};
        return undef;
    }
    return 1;
}

=item B<removeGroupGroups(I<group, group name [group name [...]]>)>

Removes the group list (string) from the first group (hash), returns 1 on
success, or undef on failure (and sets the error)

=cut

sub removeGroupGroups {
    my $group = shift;
    my @groups = @_;
    unless ($group) {
        $error = "Missing group";
        return undef;
    }
    unless (@groups) {
        $error = "Missing groups to remvoe";
        return undef;
    }
    unless ($db->removeGroupGroups($group, @groups)) {
        $error = $db->{error};
        return undef;
    }
    return 1;
}

=item B<removeGroupConfigurations(I<group, configuration [configuration [...]]>)>

Removes the configuration list (hashes) from the group (hash), returns 1 on
success, or undef on failure (and sets the error)

=cut

sub removeGroupConfigurations {
    my $group = shift;
    my @configurations = @_;
    unless ($group) {
        $error = "Missing group";
        return undef;
    }
    unless (@configurations) {
        $error = "Missing configurations to remvoe";
        return undef;
    }
    unless ($db->removeGroupConfigurations($group, @configurations)) {
        $error = $db->{error};
        return undef;
    }
    return 1;
}


=item B<getCompleteConfigurations(I<configurations ref, distribution, [bad list ref]>)>

Receives an ordered list reference of configurations and a distribution name,
and returns a complete ordered list of configuration. The configurations are
full (i.e. includes I<path> and I<dist>). The list includes:

=over

=item -

Only active configuraitons.

=item -

Intermediate configurations in the path (i.e. for /a/b/c, the list will have /a, /a/b, /a/b/c). Without any duplicates.

=for comment
=item -
Distribution variants of the same configuration.

=back

These filters are applied in that order. If the (empty) hash ref of bad list is
also supplied, it will be filled with configurations that dont' exist under the
given distribution.

=cut

sub getCompleteConfigurations {
    my @configurations = @{$_[0]};
    my $distribution = $_[1];
    my $badConfigurations = $_[2];

    @configurations = cleanInactive(@configurations);
    @configurations = fillIntermediate(@configurations);
    my @finalConfigurations = $db->getFullConfigurations(\@configurations, $distribution);

    if ($badConfigurations) {
        my @goodConfigurations = map {$_->{name}} @finalConfigurations;
        foreach my $conf (map {$_->{name}} @configurations) {
            push @$badConfigurations, $conf unless grep {$_ eq $conf} @goodConfigurations;
        }
    }
    
    return @finalConfigurations;
}

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

=item B<getGroupsByName(I<group name [group name [...]]>)>

Receives a list of groups names (strings), and returns a list of groups (order
preserved). Only existing groups are returned.

=cut

sub getGroupsByName {
    my @groups = $db->getGroupsByName(@_);
    if (@groups and not defined $groups[0]) {
        $error = $db->{error};
        return undef;
    }
    return @groups;
}

=item B<getAllHosts( )>

Returns an unordered list of all hosts. returns undef on error and sets the last error;

=cut

sub getAllHosts {
    my @hosts = $db->getAllHosts();
    if (@hosts and not defined $hosts[0]) {
        $error = $db->{error};
        return undef;
    }
    return @hosts;
}

=item B<getAllGroups( )>

Returns a sorted list of all groups. returns undef on error and sets the last error;

=cut

sub getAllGroups {
    my @groups = $db->getAllGroups();
    if (@groups and not defined $groups[0]) {
        $error = $db->{error};
        return undef;
    }
    return sort {$a cmp $b} @groups;
}

=item B<getAllDistributions( )>

Returns an ordered list of all distributions. returns undef on error and sets the last error;

=cut

sub getAllDistributions {
    my @distributions = $db->getAllDistributions();
    if (@distributions and not defined $distributions[0]) {
        $error = $db->{error};
        return undef;
    }
    return sort {$a cmp $b} @distributions;
}

=item B<getAllConfigurations(distribution)>

Returns a list of all configurations. returns undef on error and
sets the last error;

=cut

sub getAllConfigurations {
    my $distribution = shift;
    unless ($distribution) {
        $error = "Missing distribution";
        return undef;
    }
    my @configurations = $db->getAllConfigurations($distribution);
    if (@configurations and not defined $configurations[0]) {
        $error = $db->{error};
        return undef;
    }
    return sort {$a cmp $b} @configurations;
}

=item B<getLastError( )>

Returns the last error, if no error, returns the empty string. This might be set at the beginning (after use Staple), in case of an error.

=cut

sub getLastError {
    return $error;
}


=item B<getStapleDir( )>

Returns the staple directory as writen in F</etc/staple/staple_dir>. Returns C</staple> if doesn't exists.

=cut

sub getStapleDir {
    return $stapleDir;
}

=item B<getRawMounts(I<configuration [configuration [...]]>)>

Returns an ordered list of mount hashes from the given configurations. The
hashes aren't full and containes only destincation, active, and
configuration. The order is as it appear in the given configurations.

On error retures undef and sets the last error.

=cut

sub getRawMounts {
    my @configurations = @_;
    my @mounts = $db->getMounts(@configurations);
    if (@mounts and not defined $mounts[0]) {
        $error = $db->{error};
        return undef;
    }
    return @mounts;
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

=item B<getTemplates(I<configuration [configuration [...]]>)>

Returns an unordered list with templates hashes values. If same template (stage
+ destination) for different configurations, last one wins.

On error returns undef.

=cut

sub getTemplates {
    my @configurations = @_;
    unless (@configurations) {
        $error = "Missing configuration";
        return undef;
    }
    my @templates = $db->getTemplates(@configurations);
    if (@templates and not defined $templates[0]) {
        $error = $db->{error};
        return undef;
    }
    return @templates;
}

=item B<addTemplates(I<template [template [...]]>)>

Adds the given templates (hashes, contains the configurations to add
to). Previous templates with the same stage + name + configuration, will be
overridden.

If I<source> is available, it will be taken as the source for the
template. otherwise I<data> will be taken. The source will be copied, so it can
be deleted after it was added.

Returns 1 or undef.

=cut

sub addTemplates {
    my @templates = @_;
    unless (@templates) {
        $error = "Missing templates to add";
        return undef;
    }
    unless ($db->addTemplates(@templates)) {
        $error = $db->{error};
        return undef;
    }
    return 1;
}

=item B<addScripts(I<script [script [...]]>)>

Adds the given scripts (hashes, contains the configurations to add to). The
scripts will be inserted in the specified location (order).

If I<source> is available, it will be taken as the source for the
script. otherwise I<data> will be taken. The source will be copied, so it can
be deleted after it was added.

Returns 1 or undef.

=cut

sub addScripts {
    my @scripts = @_;
    unless (@scripts) {
        $error = "Missing scripts to add";
        return undef;
    }
    unless ($db->addScripts(@scripts)) {
        $error = $db->{error};
        return undef;
    }
    return 1;
}

=item B<removeScripts(I<script [script [...]]>)>

Removes the given scripts (full script hashes).

Returns 1 or undef;

=cut

sub removeScripts {
    my @scripts = @_;
    unless (@scripts) {
        $error = "Missing scripts to remove";
        return undef;
    }
    unless ($db->removeScripts(@scripts)) {
        $error = $db->{error};
        return undef;
    }
    return 1;
}

=item B<removeTemplates(I<template [template [...]]>)>

Removes the given templates (hashes with configuration hash (name +
distribution), destination and stage).

Returns 1 or undef;

=cut

sub removeTemplates {
    my @templates = @_;
    unless (@templates) {
        $error = "Missing templates to remove";
        return undef;
    }
    unless ($db->removeTemplates(@templates)) {
        $error = $db->{error};
        return undef;
    }
    return 1;
}

=item B<getScripts(I<configuration [configuration [...]]>)>

Returns an ordered list of scripts hashes. The script are ordered first by the
configurations (with the given order) and second by the internal order per
configuration.

On error undef is returned.

=cut

sub getScripts {
    my @configurations = @_;
    my @results = ();
    push @results, $db->getScripts(@configurations);
    if (@results and not defined $results[0]) {
        $error = $db->{error};
        return undef;
    }
    return @results;
}

=item B<getAutos(I<configuration [configuration [...]]>)>

Returns an ordered list of autos hashes. The autos are ordered first by the
configurations (with the given order) and second by the internal order per
configuration (like I<getScripts>).

On error undef is retuned

=cut

sub getAutos {
    my @configurations = @_;
    my @results = ();
    push @results, $db->getAutos(@configurations);
    if (@results and not defined $results[0]) {
        $error = $db->{error};
        return undef;
    }
    return @results;
}

=item B<addAutos(I<auto [auto [...]]>)>

Adds the given autos (hashes, contains the configurations to add to). The
autos will be inserted in the specified location (order).

If I<source> is available, it will be taken as the source for the
auto. otherwise I<data> will be taken. The source will be copied, so it can
be deleted after it was added.

Returns 1 or undef.

=cut

sub addAutos {
    my @autos = @_;
    unless (@autos) {
        $error = "Missing autos to add";
        return undef;
    }
    unless ($db->addAutos(@autos)) {
        $error = $db->{error};
        return undef;
    }
    return 1;
}


=item B<removeAutos(I<auto [auto [...]]>)>

Removes the given autos (full auto hashes).

Returns 1 or undef;

=cut

sub removeAutos {
    my @autos = @_;
    unless (@autos) {
        $error = "Missing autos to remove";
        return undef;
    }
    unless ($db->removeAutos(@autos)) {
        $error = $db->{error};
        return undef;
    }
    return 1;
}


=item B<whoHasGroup(I<group name>)>

Receives a single group name (string), and returns a group (hash) list, of the
groups that are attached to the given group. The output can be a group, host,
or distribution groups.
On error undef is returned, and the error is set.

=cut

sub whoHasGroup {
    my $group = shift;
    my @groups = $db->whoHasGroup($group);
    if (@groups and not defined $groups[0]) {
        $error = $db->{error};
        return undef;
    }
    return @groups;
}

=item B<whoHasConfiguration(I<configuration name>)>

Receives a single configuration name (string), and returns a group (hash) list,
of the groups that are attached to the given configuration. The output can be a
group, host, or distribution groups. The output also includes group which
contains a removed configurations.

On error undef is returned, and the error is set.

=cut

sub whoHasConfiguration {
    my @groups = $db->whoHasConfiguration(@_);
    if (@groups and not defined $groups[0]) {
        $error = $db->{error};
        return undef;
    }
    return @groups;
}

=item B<whoHasToken(I<token key, distribution>)>

Receives a single token key (string) and a distribution name, and returns a two
list refs, first of groups (can be hosts, distributions or groups), and the
second is configurations (for the given distribution). Both lists, contains
groups/configurations with values for the given token.

On error undef is returned, and the error is set.

=cut

sub whoHasToken {
    my $key = shift;
    my $distribution = shift;
    (my $groups, my $configurations) = $db->whoHasToken($key, $distribution);
    if (not defined $groups) {
        $error = $db->{error};
        return undef;
    }
    return ($groups, $configurations);
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
                $mount{$mountTokenToOption{$option}} = "";
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

The init stage from the stapleboot:

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

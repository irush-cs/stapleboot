package Staple::Misc;

#
# Copyright (C) 2007-2011 Hebrew University Of Jerusalem, Israel
# See the LICENSE file.
#
# Author: Yair Yarom <irush@cs.huji.ac.il>
#

use strict;
use warnings;
use Clone qw(clone);
use IPC::Open3;
use IO::Select;
use XML::Writer;
use XML::Parser;
use IO::File;
use IO::String;
require Exporter;


=head1 NAME

  Staple::Misc - Miscellaneous Staple utilities

=head1 SYNOPSIS

use Staple::Misc;

=head1 DESCRIPTION

Staple::Misc module

=head1 EXPORT

=over

=item versionCompare(v1, v2)

=item fixPath(path)

=item splitData(<path like string>)

=item getDirectoryList(directory)

=item cleanInactive(<list of hash refs>)

=item fillIntermediate(<list of nodes>)

=item applyTokens(data string, hash ref of tokens)

=item stageCmp(stage string a, stage string b)

=item readTokensFile(full path, [type])

=item writeTokensFile(full path, hash ref of tokens)

=item readTokensXMLFile(full path)

=item writeTokensXMLFile(full path, hash ref of tokens)

=item tokensToXML(hash ref of tokens)

=item getDistribution( )

=item runCommand(<command>)

=item invalidHost(host name)

=item invalidGroup(group name)

=item invalidDistribution(distribution name)

=item invalidConfiguration(configuration name)

=item invalidTokenKey(key name)

=back

=cut

our @ISA = qw(Exporter);
our @EXPORT_OK = qw();
our @EXPORT = qw(
                    versionCompare
                    getDistribution
                    fixPath
                    splitData
                    getDirectoryList
                    cleanInactive
                    fillIntermediate
                    applyTokens
                    stageCmp
                    readTokensFile
                    writeTokensFile
                    readTokensXMLFile
                    writeTokensXMLFile
                    tokensToXML
                    runCommand
                    invalidHost
                    invalidGroup
                    invalidDistribution
                    invalidConfiguration
                    invalidTokenKey
               );
our $VERSION = '0.2.x';


################################################################################
#   Exported
################################################################################


=head1 DESCRIPTION

=over

=item V<versionCompare(I<version1>, I<version2>)

Compares between 2 versions of staple and returns -1, 0, or 1 if version1 is
lower than, equal to, or greater than version2. "none" and undef are lower than
everything. "none" is equal to "none". (\d{3})(snap)? is the older version
style and will be commpared with the older rules.

=cut

sub versionCompare {
    my $a = shift;
    my $b = shift;

    $a = "none" if not defined $a;
    $b = "none" if not defined $b;

    return 0 if $a eq $b;

    my $oldre = qr(^(?:\d{3}(?:snap)?|none)$);

    if ($a =~ $oldre and
        $b =~ $oldre) {
        return oldVersionCompare($a, $b);
    }
    return -1 if $a =~ $oldre;
    return 1 if $b =~ $oldre;

    while ($a or $b) {
        (my $aa, my $as, $a) = $a =~ m/^([^\.-]*)([\.-])?(.*)?$/;
        (my $bb, my $bs, $b) = $b =~ m/^([^\.-]*)([\.-])?(.*)?$/;
        $aa = undef if defined $aa and $aa eq "";
        $bb = undef if defined $bb and $bb eq "";
        return 0 if (not defined $aa and not defined $bb);
        return -1 if (defined $bb and not defined $aa);
        return 1 if (defined $aa and not defined $bb);
        if ($aa =~ /^\d*$/ and $bb =~ /^\d*$/) {
            my $cc = $aa <=> $bb;
            return $cc if $cc;
            return -1 if ($bs and $as and $bs eq "." and $as ne ".");
            return 1 if ($bs and $as and $as eq "." and $bs ne ".");
            next;
        }
        my $cc = $aa cmp $bb;
        return $cc if $cc;
        return -1 if ($bs and $as and $bs eq "." and $as ne ".");
        return 1 if ($bs and $as and $as eq "." and $bs ne ".");
        next;
    }
    return 0;
}

# old style compere, \d{3}(snap)?
sub oldVersionCompare {
    my $v1 = shift;
    my $v2 = shift;

    $v1 = "none" if not defined $v1;
    $v2 = "none" if not defined $v2;
    
    return 0 if $v1 eq $v2;
    return -1 if $v1 eq "none";
    return 1 if $v2 eq "none";
    my $v1unknown = $v1 !~ m/^\d{3}(?:snap)?$/;
    my $v2unknown = $v2 !~ m/^\d{3}(?:snap)?$/;
    return $v1 cmp $v2 if $v1unknown and $v2unknown;
    return -1 if $v2unknown;
    return 1 if $v1unknown;
    
    ($v1, my $v1snap) = $v1 =~ m/^(.*?)(snap)?$/;
    ($v2, my $v2snap) = $v2 =~ m/^(.*?)(snap)?$/;

    
    my $c = $v1 <=> $v2;
    if ($c == 0) {
        return -1 if $v1snap and not defined $v2snap;
        return 1 if $v2snap and not defined $v1snap;
    }
    return $c / abs($c);
}


=item B<getDistribution( )>

Tries to find out what distribution are we running on with the following methods:
1. /etc/staple/distribution
2. /proc/cmdline (nfsroot= option)

If fails, undef is returned

=cut

sub getDistribution {
    if (-r "/etc/staple/distribution") {
        open(FILE, "/etc/staple/distribution");
        my $dist = <FILE>;
        close(FILE);
        chomp $dist;
        return $dist;
    } elsif (-r "/proc/cmdline") {
        open(FILE, "/proc/cmdline");
        my $cmdline = <FILE>;
        close(FILE);
        $cmdline =~ s!^nfsroot=[^\s]*?/([^/,]*?)[\s,].*$!$1!;
        if ($cmdline) {
            chomp $cmdline;
            return $cmdline;
        }
    }
    return undef;
}

=item B<runCommnad(I<command>)>

Executes the given commnad, and returns the exit code (wait >> 8), the command
output, the command error, and the command output and error combind (4 scalars).

=cut

sub runCommand {
    my $command = shift;
    my $outputStream = shift;
    local $| = 1;
    my $commandOutput = "";
    my $commandError = "";
    my $commandOutputError = "";
    my $exitCode;
    my $pid = open3(\*WTRFH, \*RDRFH, \*ERRFH, "$command");
    close(WTRFH);
    my $selector = IO::Select->new();
    $selector->add(*RDRFH, *ERRFH);

    my $buf;
    while (my @ready = $selector->can_read) {
        foreach my $fh (@ready) {
            if (fileno($fh) == fileno(RDRFH)) {
                $buf = <RDRFH>;
                $commandOutput .= $buf if $buf;
            } else {
                $buf = <ERRFH>;
                $commandError .= $buf if $buf;
            }
            if ($buf) {
                $commandOutputError .= $buf;
                print $outputStream $buf if $outputStream;
            }
            $selector->remove($fh) if eof($fh);
        }
    }
    waitpid($pid, 0);
    $exitCode = $? >> 8;
    close(RDRFH);
    close(ERRFH);
    return ($exitCode, $commandOutput, $commandError, $commandOutputError);
}

=item B<fixPath(I<path>)>

Returns the I<path> without any duplicates '/' or annoying '/..'.

=cut

sub fixPath {
    my $path = $_[0];
    $path =~ s!/+!/!g;
    while ($path =~ s!/[^/]+/\.\./!/!) {};
    $path =~ s!/[^/]+/\.\.$!/!;
    return $path;
}


=item B<getDirectoryList(I<path>)>

FIXME (I would have left this line empty, put it somewhat ruins the formatting. And yes, this is a complaint)

=cut

sub getDirectoryList {
    my $currentDir = $_[0];
    my @results = ();
    unless (opendir(DIR, $currentDir)) {
        return ();
    }
    @results = grep { !/(^\.$)|(^\.\.$)/ } readdir(DIR);
    closedir DIR;
    chomp @results;
    @results = map { "$currentDir/$_" } @results;
    my @recursive = ();
    foreach my $file (@results) {
        push @recursive, getDirectoryList($file) if -d $file and not -l $file;
    }
    push @results, @recursive;
    return @results;
}


=item B<splitData(I<path>, [I<seperator>])>

Returns a list of all direcories up to I<path>. There will not be an empty
entry (when I<path> starts with the seperator). Entries will not end with the
seperator. By default I<seperator> is "/". e.g. splitData("/a/b/c") ->
("/a","/a/b","/a/b/c").

=cut

sub splitData {
    my $data = $_[0];
    my $seperator = "/";
    $seperator = $_[1] if $_[1];    
    my @output = ();

    my @splited = split /${seperator}/, $data;
    my $path = "";
    for my $split (@splited) {
        #next unless $split;
        $path .= "$split";
        push @output, $path;
        $path .= "$seperator";
    }
    shift @output unless $output[0];
    return @output;
}

=item B<cleanInactive(I<E<lt>list of hash refsE<gt>>)>

Receives an ordered list of hashes with "name" and "active" keys. Where the
"name" is a path-like string and "active" is either 1 or 0. It returns an
ordered list of only active values (same hashes as given). Inactive values will
remove their predecessors starting with the same "name" as their own.

=cut

sub cleanInactive {
    my @rawData = @_;
    my @data = ();
    my %badData = ();
    foreach my $data (@rawData) {
        $badData{$data->{name}}++ unless ($data->{active});
    }
    foreach my $data (@rawData) {
        if ($data->{active}) {
            push @data, $data unless grep {"$data->{name}" eq $_ or
                                             "$data->{name}" =~ m/^$_\//} keys %badData;
        } else {
            $badData{$data->{name}}--;
            delete $badData{$data->{name}} unless $badData{$data->{name}};
        }
    }
    return @data;
}

=item B<fillIntermediate(I<E<lt>list of nodesE<gt>>)>

Receives an ordered list of nodes, where the "name" attribute is a path-like
string. It returns an ordered list with intermediate nodes (before the final
one). The new values are deep duplicates of the originals with "name" value
changed appropriately. No duplicate entries are returned. If name can't be
changed, the failed intermediate node is dropped.

=cut

sub fillIntermediate {
    my @compactData = @_;
    my @data = ();
    my %data = ();
    foreach my $data (@compactData) {
        my @splited = splitData $data->name();
        for my $split (@splited) {
            unless ($data{join(":", $data->type(), $split)}) {
                my $newData = clone($data);
                push @data, $newData if $newData->name($split);
                $data{join(":", $data->type(), $split)} = 1;
            }
        }
    }
    return @data;
}

=item B<applyTokens(I<data string, hash ref of tokens>)>

Receives a string, and a tokens hash ref. Returns the string with applied tokens

=cut

sub applyTokens {
    my $data = $_[0];
    my %tokens = %{$_[1]};
    foreach my $token (keys %tokens) {
        $data =~ s/\Q$token\E/$tokens{$token}->{value}/g;
    }
    return $data;
}

=item B<stageCmp(I<stage a, stage b>)>

Returns -1, 0 or 1 if a less then, equal or larger than b (auto < mount < sysinit < final).

=cut

my $stageComparisonChart = {auto    => {auto => 0, mount => -1, sysinit => -1, final => -1},
                            mount   => {auto => 1, mount =>  0, sysinit => -1, final => -1},
                            sysinit => {auto => 1, mount =>  1, sysinit =>  0, final => -1},
                            final   => {auto => 1, mount =>  1, sysinit =>  1, final =>  0}};

sub stageCmp {
    my $a = shift;
    my $b = shift;
    return $stageComparisonChart->{$a}->{$b};
}

=item B<readTokensFile(I<full path, [type]>)>

Returns a tokens hash, with value = raw, and type = "static" (if not given)

=cut

sub readTokensFile {
    my $file = shift;
    my $type = "static";
    $type = shift if $_[0];
    my %tokens = ();
    if (open(FILE, "<$file")) {
        my @tokens = split /;\n/, join "", <FILE>;
        close(FILE);
        my %rawTokens = map {/^(.*?)=(.*)$/s} @tokens;
        @tokens{keys %rawTokens} = map {{key => $_, value => $rawTokens{$_}, raw => $rawTokens{$_}, type => $type, source => "file:$file"}} keys %rawTokens;
    }
    return %tokens;
}

=item B<writeTokensFile(I<full path, hash ref of tokens>)>

Writes the tokens to file. Returns 1 on success or undef on failure. Writes the
"raw" attribute of the token.

=cut

sub writeTokensFile {
    my $file = shift;
    my $tokens = shift;
    return undef unless ($file and $tokens);
    if (open(FILE, ">$file")) {
        my $data = join "", map {"$_->{key}=$_->{raw};\n"} sort {$a->{key} cmp $b->{key}} values %$tokens;
        print FILE $data;
        close(FILE);
        return 1;
    }
    return undef;
}

=item B<readTokensXMLFile(I<full path, [keep source]>)>

Returns a tokens hash, with value = raw if either is missing (though there
shouldn't be "value" in a file), and type = static if missing (though it
should be in an xml file format). Returns undef on error.

if I<keep source> is true, and the tokens in the file have "source", the source
will be kept. otherwise, the source is I<file:path>. The default is false.

=cut

sub readTokensXMLFile {
    my $file = shift;
    my $keepsource = shift;
    return undef unless $file and -e $file;
    #my $tokens = XMLin($file, "keyattr" => {"token" => "+key"}, "forcearray" => ["token"]);
    my $tokens = _parseXML($file);
    return undef unless $tokens;
    #$tokens = $tokens->{token};
    foreach my $key (keys %{$tokens}) {
        $tokens->{$key}->{value} = "" unless exists $tokens->{$key}->{value} or exists $tokens->{$key}->{raw};
        $tokens->{$key}->{raw} = $tokens->{$key}->{value} unless exists $tokens->{$key}->{raw};
        $tokens->{$key}->{value} = $tokens->{$key}->{raw} unless exists $tokens->{$key}->{value};
        $tokens->{$key}->{type} = "static" unless exists $tokens->{$key}->{type};
        $tokens->{$key}->{source} = "file:$file" unless exists $tokens->{$key}->{source} and $keepsource;
    }
    return %$tokens;
}

=item B<writeTokensXMLFile(I<full path, hash ref of tokens>)>

Writes the tokens to an xml file. Returns 1 on success or undef on
failure. Writes only the "key" "type" and "raw" attributes.

=cut

sub writeTokensXMLFile {
    my $file = shift;
    my $tokens = shift;
    unless (%$tokens) {
        unlink "$file";
        return 1;
    }
    return undef unless ($file and $tokens);
    my $xml = tokensToXML($tokens, [qw(key raw type)]);
    return undef unless $xml;
    if (open(FILE, ">$file")) {
        print FILE $xml;
        close(FILE);
        chmod 0644, $file;
        return 1;
    }
    return undef;
}


=item B<tokensToXML(I<hash ref of tokens, [list ref of token attributes]>)>

Converts the tokens hash ref to XML string. Returns undef on error. The list
ref is or attributes will determine which attributes will be printed in the
xml. By default all attributes are printed.

=cut

sub tokensToXML {
    my $tokens = shift;
    my $attr = shift;
    return undef unless ($tokens);
    my $string = "";
    my $io = new IO::String($string);
    my $writer = new XML::Writer(DATA_MODE => 1, DATA_INDENT => 4, OUTPUT => $io);
    $writer->xmlDecl();
    $writer->startTag("tokens");
    foreach my $token (sort {$a->{key} cmp $b->{key}} values %$tokens) {
        $writer->startTag("token");
        my $cattr = $attr;
        $cattr = [keys %$token] unless $cattr;
        for my $elem (sort {$a cmp $b} @$cattr) {
            $writer->dataElement($elem, $token->{$elem}) if exists $token->{$elem};
        }
        $writer->endTag("token");
    }
    $writer->endTag("tokens");
    $writer->end();
    return $string;
}

=item B<invalidHost(I<host name>)>

Checks whether the given host name is invalid. Currently host name can't
contain spaces or slashes, and can't equal to "." or "..". Returns empty string
if the host is legal, or an error message if the host name is invalid legal.

=cut

sub invalidHost {
    my $host = shift;
    return "Missing host name" if not defined $host or length($host) == 0;
    return "Host can't contain '.' or '..'" if $host eq ".." or $host eq ".";
    return "Host can't contain '/'" if index($host, "/") >= 0;
    return "Host can't contain spaces" if $host =~ m/\s/;
    return "";
}

=item B<invalidGroup(I<group name>)>

Checks whether the given group name is invalid. Returns the empty string if the
group is legal, or an error mesage if the group name is invalid.

=cut

sub invalidGroup {
    my $group = shift;
    return "Missing group name" if not defined $group or length($group) == 0;
    return "Group must start with '/'" if (index($group, "/") != 0);
    return "Group can't end with '/'" if (rindex($group, "/") == length($group) - 1);
    return "Group can't contain '..'" if $group =~ m,/\.\./|/\.\.$,;
    return "Group can't contain '.'" if $group =~ m,/\./|/\.$,;
    return "";
}

=item B<invalidDistribution(I<distribution name>)>

Checks whether the given distribution name is invalid. Returns the empty string
if the distribution is legal, else returns an error mesage.

=cut

sub invalidDistribution {
    my $distribution = shift;
    return "Missing distribution name" if not defined $distribution or length($distribution) == 0;
    return "Distribution can't contain '/'" if index($distribution, "/") >= 0;
    return "Distribution can't be '.' or '..'" if $distribution eq "." or $distribution eq "..";
    return "";
}


=item B<invalidConfiguration(I<configuration name>)>

Checks whether the given configuration name is invalid. Returns an empty string
if the configuration is legal, otherwise returns an error mesage.

=cut

sub invalidConfiguration {
    my $configuration = shift;
    return "Missing configuration name" if not defined $configuration or length($configuration) == 0;
    return "Configuration must start with '/' or 'common/' (not $configuration)" if (index($configuration, "/") != 0 and index($configuration, "common/") != 0);
    return "Configuration can't end with '/'" if (rindex($configuration, "/") == length($configuration) - 1);
    return "Configuration can't contain '..'" if $configuration =~ m,/\.\./|/\.\.$,;
    return "Configuration can't contain '.'" if $configuration =~ m,/\./|/\.$,;
    return "";
}

=item B<invalidTokenKey(I<key name>)>

Checks whether the given string is a valid token key. Returns an empty string
if the key is valid, otherwise returns an error message.

=cut

sub invalidTokenKey {
    my $key = shift;
    return "Missing key" if not defined $key or length($key) == 0;
    return "Key can't contain '='" if (index($key, "=") >= 0);
    return "Key can't contain ';\\n'" if (index($key, ";\n") >= 0);
    return "";
}


################################################################################
#   XML handlers
################################################################################

my @_XML_tokenKeys = ("key", "raw", "value", "type", "source");

our $_XML_tokens = {};
our $_XML_currentToken = {};
our $_XML_currentString = "";

sub _parseXML {
    (my $file) = @_;

    # the XML::Parser doesn't seem to be able to pass arguments around. So I'll
    # "simply" make them local.
    local $_XML_tokens = {};
    local $_XML_currentToken = {};
    local $_XML_currentString = "";
    my $parser = new XML::Parser(Handlers => {
                                              Init => \&_parseXMLInit,
                                              Final => \&_parseXMLFinal,
                                              Start => \&_XMLstartTag,
                                              End => \&_XMLendTag,
                                              Char => \&_XMLcharData,
                                             });
    my $tokens = eval {$parser->parsefile($file)};
    return undef if $@;
    return $tokens;
}

sub _parseXMLInit {
    (my $expat) = @_;
    $_XML_tokens = {};
    $_XML_currentToken = {};
}

sub _parseXMLFinal {
    (my $expat) = @_;
    return $_XML_tokens;
}

sub _XMLstartTag {
    my($expat, $element, %attrs) = @_;
    $_XML_currentString = "";
    if ($element eq "tokens") {
        $_XML_tokens = {};
        $_XML_currentToken = {};
    } elsif ($element eq "token") {
        $_XML_currentToken = {};
    } elsif (grep {$_ eq $element} @_XML_tokenKeys) {
    } else {
        print "start: $element\n";
    }
}

sub _XMLendTag {
    my($expat, $element) = @_;
    if ($element eq "tokens") {
    } elsif ($element eq "token") {
        $_XML_tokens->{$_XML_currentToken->{key}} = $_XML_currentToken if $_XML_currentToken->{key} and $_XML_currentToken->{type};
        $_XML_currentToken = {};
    } elsif (grep {$_ eq $element} @_XML_tokenKeys) {
        $_XML_currentToken->{$element} = $_XML_currentString;
    } else {
        print "end: $element\n";
    }
    $_XML_currentString = "";
}

sub _XMLcharData {
    my($expat, $string) = @_;
    $_XML_currentString .= $string;
}

################################################################################
#   The end
################################################################################

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

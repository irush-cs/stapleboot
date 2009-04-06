package Staple::Misc;

#
# Copyright (C) 2007-2009 Hebrew University Of Jerusalem, Israel
# See the LICENSE file.
#
# Author: Yair Yarom <irush@cs.huji.ac.il>
#

use strict;
use warnings;
use IPC::Open3;
use XML::Writer;
use XML::Simple;
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

=item fixPath(path)

=item splitData(<path like string>)

=item getDirectoryList(directory)

=item cleanInactive(<list of hash refs>)

=item fillIntermediate(<list of hash refs>)

=item applyTokens(data string, hash ref of tokens)

=item stageCmp(stage string a, stage string b)

=item readTokensFile(full path, [type])

=item writeTokensFile(full path, hash ref of tokens)

=item readTokensXMLFile(full path)

=item writeTokensXMLFile(full path, hash ref of tokens)

=item tokensToXML(hash ref of tokens)

=item getDistribution()

=item runCommand(<command>)

=back

=cut

our @ISA = qw(Exporter);
our @EXPORT_OK = qw();
our @EXPORT = qw(
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
               );
our $VERSION = '002';


################################################################################
#   Exported
################################################################################


=head1 DESCRIPTION

=over

=item B<getDistribution()>

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
output, and the command error (three strings).

=cut

sub runCommand {
    my $command = shift;
    my $commandOutput;
    my $commandError;
    my $exitCode;
    my $pid = open3(\*WTRFH, \*RDRFH, \*ERRFH, "$command");
    waitpid($pid, 0);
    $commandOutput = join "", <RDRFH>;
    $commandError = join "", <ERRFH>;
    $exitCode = $? >> 8;
    close(RDRFH);
    close(WTRFH);
    close(ERRFH);
    return ($exitCode, $commandOutput, $commandError);
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

=item B<fillIntermediate(I<E<lt>list of hash refsE<gt>>)>

Receives an ordered list of hashes with a "name" key. Where "name" is a
path-like string. It returns an ordered list with intermediate hashes (before
the final one). The new values are simply duplicates of the originals with
"name" value changed appropriately. No duplicate entries are returned.

=cut

sub fillIntermediate {
    my @compactData = @_;
    my @data = ();
    my %data = ();
    foreach my $data (@compactData) {
        my @splited = splitData $data->{name};
        for my $split (@splited) {
            unless ($data{$split}) {
                my %newData = %$data;
                $newData{name} = $split;
                push @data, \%newData;
                $data{$split} = 1;
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
        $data =~ s/$token/$tokens{$token}->{value}/g;
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

=item B<readTokensFile(I<full path, [type]>)

Returns a tokens hash, with value = raw, and type = "unknown" (if not given)

=cut

sub readTokensFile {
    my $file = shift;
    my $type = "unknown";
    $type = shift if $_[0];
    my %tokens = ();
    if (open(FILE, "<$file")) {
        my @tokens = split /;\n/, join "", <FILE>;
        close(FILE);
        my %rawTokens = map {/^(.*?)=(.*)$/s} @tokens;
        @tokens{keys %rawTokens} = map {{key => $_, value => $rawTokens{$_}, raw => $rawTokens{$_}, type => $type}} keys %rawTokens;
    }
    return %tokens;
}

=item B<writeTokensFile(I<full path, hash ref of tokens>)

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

=item B<readTokensXMLFile(I<full path>)

Returns a tokens hash, with value = raw if either is missing (though there
shouldn't be "value" in a file), and type = unknown if missing (though it
should be in an xml file format). Returns undef on error.

=cut

sub readTokensXMLFile {
    my $file = shift;
    return undef unless $file and -e $file;
    my $tokens = XMLin($file, "keyattr" => {"token" => "+key"}, "forcearray" => ["token"]);
    return undef unless $tokens;
    $tokens = $tokens->{token};
    foreach my $key (keys %{$tokens}) {
        $tokens->{$key}->{raw} = $tokens->{$key}->{value} unless exists $tokens->{$key}->{raw};
        $tokens->{$key}->{value} = $tokens->{$key}->{raw} unless exists $tokens->{$key}->{value};
        $tokens->{$key}->{type} = "unknown"
    }
    return %$tokens;
}

=item B<writeTokensXMLFile(I<full path, hash ref of tokens>)

Writes the tokens to an xml file. Returns 1 on success or undef on
failure. Writes only the "key" "type" and "raw" attributes.

=cut

sub writeTokensXMLFile {
    my $file = shift;
    my $tokens = shift;
    return undef unless ($file and $tokens);
    my $xml = tokensToXML($tokens, [qw(key raw type)]);
    return undef unless $xml;
    if (open(FILE, ">$file")) {
        print FILE $xml;
        close(FILE);
        return 1;
    }
    return undef;
}


=item B<tokensToXML(I<hash ref of tokens, [list ref of token attributes]>)

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
            $writer->dataElement($elem, $token->{$elem}) if $token->{$elem};
        }
        $writer->endTag("token");
    }
    $writer->endTag("tokens");
    $writer->end();
    return $string;
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

Copyright (C) 2007-2009 Hebrew University Of Jerusalem, Israel
See the LICENSE file.

=cut

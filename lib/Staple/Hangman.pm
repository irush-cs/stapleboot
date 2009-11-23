package Staple::Hangman;

#
# Copyright (C) 2007-2009 Hebrew University Of Jerusalem, Israel
# See the LICENSE file.
#
# Author: Yair Yarom <irush@cs.huji.ac.il>
#

use strict;
use warnings;
use Term::ReadKey;

require Exporter;

our @ISA = qw(Exporter);
our @EXPORT_OK = qw();
our @EXPORT = qw(
                    hangman
               );
our $VERSION = '004';


=head1 NAME

  Staple::Hangman - hangman

=head1 DESCRIPTION

  Hangman.

=cut

my $gotSigInt = 0;


# prints the ascii "art"
# input: number of wrong guesses
sub print_podium {
    my $guesses = shift;
    print "\n";
    print "     ______ \n";
    print "     |    | \n";
    print "     |    ";
    print "0" if $guesses > 0;
    print "\n";
    print "     |   ";
    print $guesses > 4 ? "/" : " ";
    print $guesses > 1 ? "|" : " ";
    print $guesses > 6 ? "\\" : " ";
    print "\n";
    print "     |    ";
    print "|" if $guesses > 2;
    print "\n";
    print "     |   ";
    print $guesses > 3 ? "/ " : "  ";
    print "\\" if $guesses > 5;
    print "\n";
    print "   __|_____   \n";
    print "   |      |___\n";
    print "   |_________|\n";
};

# guess a word
# input: a word (lc, [a-z]+), an array hash with failed attempts
# output: 0 on failer, 1 on success
sub guess {
    my $word = shift;
    my @failed = @{$_[0]};
    my %guessed = ();
    my %remaining = ();
    @remaining{split //, $word} = split //, $word;
    my $bad = 0;

    ReadMode 4;
    while (defined (ReadKey(-1))) {}
    ReadMode 0;

    while (1) {
        print `tput cup 0 0`;
        print `tput clear`;
        print "\n";
        print "Past failures: ".join(", ", @failed) if @failed;
        print "\n";
        print_podium $bad;
        print "\n   Word: ";
        foreach my $letter (split //, $word) {
            if ($guessed{$letter} || $bad > 6) {
                print "$letter";
            } else {
                print "-";
            }
        }
        print "\n";
        print "Guessed: ".join("", sort {$a cmp $b} keys %guessed)."\n";

        return 0 if ($bad > 6);
        return 1 unless %remaining;
        
        ReadMode 3;
        my $key;
        while (not defined ($key = ReadKey(-1))) {last if $gotSigInt;}
        ReadMode 0;
        return 0 if $gotSigInt;
        next unless ($key =~ m/^[a-z]$/);
        
        $bad++ unless delete $remaining{$key} || $guessed{$key};
        $guessed{$key} = 1;
    }
    return 1;
}

# hangman
# output, number of failed attempts, or -1 on error
sub hangman {
    my @failed = ();
    $gotSigInt = 0;
    
    my $dict = "/usr/share/dict/words";

    unless (-r $dict) {
        print STDERR "can't read $dict\n";
        return -1;
    }

    unless (open(DICT, "<$dict")) {
        print STDERR "can't open $dict: $!\n";
        return -1;
    }
    my @words = <DICT>;
    close(DICT);
    my $sigInt = $SIG{INT};
    $SIG{INT} = sub {$gotSigInt = 1;};


    chomp(@words);
    @words = grep {m/^[a-zA-Z]{4,}$/} @words;

    print scalar(@words);
    print " words found\n";

    while (@words) {
        my $word = $words[rand(@words)];
        @words = grep {$_ ne $word} @words;
        last if guess lc($word), [@failed];
        push @failed, lc($word);
        last if $gotSigInt;
        sleep 1;
    }

    print "\nYou wuss!\n\n" if $gotSigInt;
    if (defined $sigInt) {
        $SIG{INT} = $sigInt;
    } else {
        $SIG{INT} = 'DEFAULT';
    }
    return scalar(@failed);
}


1;


__END__

=head1 AUTHOR

Yair Yarom, E<lt>irush@cs.huji.ac.ilE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2007-2009 Hebrew University Of Jerusalem, Israel
See the LICENSE file.

=cut

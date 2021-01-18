#!/usr/bin/perl -w
use strict;

# by Marco Gnecchi

use lib 'modules';

use DBI;
use DateTime;
use Data::Dumper;
use List::MoreUtils qw(uniq);
use File::HomeDir;

use Decoder;

# default path
#------------------------------------------------------------------------------------------------------------
# my $path = File::HomeDir->my_desktop . File::Util::SL . 'caricamentoDC' . File::Util::SL;
#my $path = 'C:\Users\utente.quadrature\Desktop\caricamentoDC';
my $path = '/Users/if65/Desktop/caricamentoDC';
unless(-e $path or mkdir $path) {die "Impossibile creare la cartella $path: $!\n";};

# elenco file presenti nella directory di default
#------------------------------------------------------------------------------------------------------------
my @elencoFiles;
opendir my($DIR), $path or die "Non ? stato possibile aprire la directory $path: $!\n";
@elencoFiles = sort {$a cmp $b} grep { /^\d{4}_\d{8}_\d{6}_DC\.TXT$/ } readdir $DIR;
closedir $DIR;
        
foreach my $fileName (@elencoFiles) {
	my $store = '';
    my $year = '';
    my $month = '';
    my $day = '';
    if ($fileName =~ /^(\d{4})_\d{2}(\d{2})(\d{2})(\d{2})/) {
		$store = $1;
        $year = quotemeta($2);
        $month = quotemeta($3);
        $day = quotemeta($4);
	}
    
    my @dc = ();
    if (open my $fileHandler, "<:crlf", $path . "\\" . $fileName) {
        print "Caricamento negozio: $store, del 20$year-$month-$day\n";
        
        my $line;
        my $sequenceNumber = 0;
        my $transstep = 0;
        while (!eof($fileHandler)) {
            $line = <$fileHandler> ;
            $line =~  s/\n$//ig;
            
            if ($line =~ /^(\d{4}):(\d{3}):($year)($month)($day):(\d{2})(\d{2})(\d{2}):(\d{4}):...:(.):(.{3}):(.{4}):(.{16})(.{19})$/ ) {
                my $reg = $2 * 1;
                # my $store = $1;
                my $ddate = '20' . $3 . '-' . $4 . '-' . $5;
                my $ttime = $6 . $7 . $8;
                my $trans = $9;
                my $recordType = $10;
                my $recordCode = $11;
                my $userno = $12 * 1;
                my $misc = $13;
                my $data = $14;
                
                my @row = ();
                push @row, $reg, $store, $ddate, $ttime, ++$sequenceNumber, $trans, ++$transstep, $recordType, $recordCode, $userno, $misc, $data;
                
                if ($recordType =~ /F/) {
                    $transstep = 0;
                }
                
                push @dc, \@row;
            }
            
        }
        close($fileHandler);
    } else {
        print "Annullato caricamento negozio: $store, del 20$year-$month-$day\n";
    }
                    
    # se il @dc contiene righe posso caricare i dati
    if (@dc) {
        &loadDC(\@dc);
    }
}


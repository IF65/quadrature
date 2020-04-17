#!/usr/bin/perl -w
use strict;

# by Marco Gnecchi

use DBI;
use DateTime;

# date
#------------------------------------------------------------------------------------------------------------
my $dataCorrente 	= DateTime->now(time_zone=>'CET');
my $oraCorrente 	= DateTime->now(time_zone=>'CET');
my $dataInizio	    = DateTime->new(year=>2020, month=>2, day=>25);

# connessione al database quadrature per recuperare l'a lista dei negozi'ultima data caricata
#------------------------------------------------------------------------------------------------------------
my $ip      = '10.11.14.128';
my $user    = 'root';
my $pw      = 'mela';
#my $dbh = DBI->connect("DBI:mysql:mysql:$ip", $user, $pw);
my $dbh = DBI->connect("dbi:ODBC:Driver={MySQL ODBC 5.3 Unicode Driver};Server=$ip;UID=$user;PWD=$pw");
if (! $dbh) {
    die "Errore durante la connessione al database Quadrature ($ip)!\n";
}
my $sth = $dbh->prepare(qq{
        CREATE TABLE IF NOT EXISTS `mtx`.`eod` (
        `store` varchar(4) NOT NULL DEFAULT '',
        `ddate` date NOT NULL,
        `storeDescription` varchar(100) DEFAULT NULL,
        `itemCount` int(11) NOT NULL DEFAULT 0,
        `totalAmount` decimal(11,2) NOT NULL DEFAULT 0.00,
        `lastSequenceNumber` int(11) NOT NULL DEFAULT 0,
        `status` tinyint(4) NOT NULL DEFAULT 0,
        `eod` tinyint(4) NOT NULL DEFAULT 0,
        `ip` varchar(15) NOT NULL DEFAULT '',
        `created_at` timestamp NULL DEFAULT NULL,
        `modified_at` timestamp NULL DEFAULT NULL,
        PRIMARY KEY (`store`,`ddate`),
        KEY `eod` (`eod`)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
});
if (! $sth->execute()) {
    die "Creazione tabella edo del db quadrature ($ip) fallita!\n";
}

$sth = $dbh->prepare(qq{
    select ifnull(max(ddate),'0000-00-00') lastDate from mtx.eod ;
});
my $ultimaDataCaricata;
if ($sth->execute()) {
    if (my @row = $sth->fetchrow_array()) {
        if ($row[0] =~ /^(\d{4})\-(\d{2})\-(\d{2})$/) {
            if ($1 eq '0000') {
				$ultimaDataCaricata = $dataInizio->clone();
			} else {
                $ultimaDataCaricata = DateTime->new(year=>$1, month=>$2, day=>$3);
            }
        }
    }   	
} else {
    die "Errore una query su Quadrature ($ip)!\n";
}

# connessione al database archivi per recuperare la lista dei negozi
#------------------------------------------------------------------------------------------------------------
my $ipDbArchivi    = '10.11.14.78';
my $userDbArchivi  = 'root';
my $pwDbArchivi    = 'mela';
#my $dbhArchivi = DBI->connect("DBI:mysql:mysql:$ipDbArchivi", $userDbArchivi, $pwDbArchivi);
my $dbhArchivi = DBI->connect("dbi:ODBC:Driver={MySQL ODBC 5.3 Unicode Driver};Server=$ipDbArchivi;UID=$userDbArchivi;PWD=$pwDbArchivi");
if (! $dbhArchivi) {
    die "Errore durante la connessione al database Archivi ($ipDbArchivi)!\n";
}
my $sthArchivi = $dbhArchivi->prepare(qq{
    select n.`codice`, n.`negozio_descrizione`, n.`ip_mtx`
    from archivi.negozi as n
    where
        n.`data_inizio`<=? and (n.`data_fine` is null or n.`data_fine`>=?) and
        n.`societa` in ('02','05') and n.`codice` not like '00%' and n.`abilita`=1;
});

# inserimento dei lavori da eseguire
#------------------------------------------------------------------------------------------------------------
$sth = $dbh->prepare(qq{insert ignore into mtx.eod (ddate, store, storeDescription, ip, created_at, modified_at) values (?,?,?,?, now(), now());});
my $dataInUso = $ultimaDataCaricata->clone();
while (DateTime->compare_ignore_floating( $dataInUso, $dataCorrente ) <= 0) {
    if ($sthArchivi->execute($dataInUso->ymd('-'),$dataInUso->ymd('-'))) {
        while(my @row = $sthArchivi->fetchrow_array()) {
            $sth->execute($dataInUso->ymd('-'), $row[0], $row[1], $row[2]);
        }
    }
    
    $dataInUso->add( days => 1 );
}

#   status (flag)
#   0 = caricamento dati in attesa di cominciare o in corso 
#   1 = caricamento in corso ma giornata fiscalmente chiusa (dati spostati da idc => idc_eod su mtx)
#   2 = giornata fiscalmente chiusa e completamente caricata (è verificato che l'ultimo sequencenumber sia già stato caricato)
#   3 = negozio non aperto al pubblico.
#
#   eod (flag)
#   0 = la giornata è in caricamento o conclusa e il calcolo finale non è ancora stato fatto
#   1 = la giornata è conclusa ed è stato fatto il calcolo finale degli importi
#
# cerco i negozi che non abbiano ancora fatto il calcolo finale (eod = 0)
#------------------------------------------------------------------------------------------------------------
$sth = $dbh->prepare(qq{
    update mtx.eod as e join (select store, ddate, count(*) itemCount, sum(totalAmount) totalAmount, max(sequencenumber) maxSequenceNumber
    from mtx.idc where recordtype = 'F' and recordcode1 = '1' group by 1,2) as i on e.store=i.store and e.ddate = i.ddate 
    set e.`itemCount`=i.itemCount, e.`totalAmount`=i.totalAmount, e.`lastSequenceNumber`=i.maxSequenceNumber
    where e.eod = 0 limit 200;
}); # limito a 200 record alla volta per evitare: [The total number of locks exceeds the lock table size] (memory overflow)
$sth->execute();
$sth->finish();

# cerco i negozi chiusi (status >= 2) che non abbiano ancora fatto il calcolo finale (eod = 0)
#------------------------------------------------------------------------------------------------------------
$sth = $dbh->prepare(qq{
    update mtx.eod as e join (select store, ddate, count(*) itemCount, sum(totalAmount) totalAmount, max(sequencenumber) maxSequenceNumber
    from mtx.idc where recordtype = 'F' and recordcode1 = '1' group by 1,2) as i on e.store=i.store and e.ddate = i.ddate 
    set e.`itemCount`=i.itemCount, e.`totalAmount`=i.totalAmount, e.`lastSequenceNumber`=i.maxSequenceNumber, e.eod=1
    where e.status >= 2 and e.eod = 0 limit 200;
}); # limito a 200 record alla volta per evitare: [The total number of locks exceeds the lock table size] (memory overflow)
$sth->execute();
$sth->finish();






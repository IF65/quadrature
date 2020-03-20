#!/usr/bin/perl -w
use strict;

# by Marco Gnecchi

use DBI;
use DateTime;
use Data::Dumper;
use Log::Log4perl;
use threads;
use threads::shared;
use List::MoreUtils qw(uniq);

# date
#------------------------------------------------------------------------------------------------------------
my $dataCorrente 	= DateTime->now(time_zone=>'CET');
my $oraCorrente 	= DateTime->now(time_zone=>'CET');
my $dataInizio	    = DateTime->new(year=>2020, month=>3, day=>1);

# connessione al database quadrature per recuperare la lista dei negozi
#------------------------------------------------------------------------------------------------------------
my $ip      = '10.11.14.128';
my $user    = 'root';
my $pw      = 'mela';
#my $dbh = DBI->connect("DBI:mysql:mysql:$ip", $user, $pw);
my $dbh = DBI->connect("dbi:ODBC:Driver={MySQL ODBC 5.3 Unicode Driver};Server=$ip;UID=$user;PWD=$pw");
if (! $dbh) {
    die "Errore durante la connessione al database Quadrature ($ip)!\n";
}

my $semaforo = 0;
my $sth = $dbh->prepare("select ifnull(count(*),0) from `log`.`semaforo` where tipo =  1");
if ($sth->execute()) {
    $semaforo = $sth->fetchrow_array();
}
$sth->finish();

if (! $semaforo) {
    $sth = $dbh->prepare("insert into `log`.`semaforo` (`tipo`,`stato`) values (1, 100)");
    $sth->execute();
    $sth->finish();
    
    # cerco le date in cui ci siano giornate da caricare
    my @dateCanGiornateDaCaricare = ();
    my $sth = $dbh->prepare("select distinct ddate from mtx.eod where status < 2 order by 1");
    if ($sth->execute()) {
        while (my $data = $sth->fetchrow_array()) {
            push @dateCanGiornateDaCaricare, $data;
        }
    }
    
    foreach( @dateCanGiornateDaCaricare ){
        my %negoziDaCaricare = ();
        my $sth = $dbh->prepare("select store, storeDescription, ip from mtx.eod where status < 2 and ddate = ? order by 1");
        if ($sth->execute($_)) {
            while (my @row = $sth->fetchrow_array()) {
                $negoziDaCaricare{$row[0]} = {'descrizione' => $row[1], 'ip' => $row[2]};
            }
        }
        $sth->finish();
        
        my @negozi = keys %negoziDaCaricare;
        
        my @thr =();
        for (my $i=0; $i<@negozi; $i++) {
            push @thr, threads->create('GetFiles', $negozi[$i], $negoziDaCaricare{$negozi[$i]}{'ip'}, $_);
            #&GetFiles('3654', '192.168.154.11', $_); #DEBUG ONLY
        }
        
        #con l'istruzione join faccio in modo che l'esecuzione aspetti fino a che l'ultimo thread sia terminato
        for (my $j=0; $j<@thr; $j++) {
            $thr[$j]->join();
        }
    }
   
    $sth = $dbh->prepare("delete from `log`.`semaforo` where tipo = 1;");
    $sth->execute();
    $sth->finish();
}

$dbh->disconnect();

exit;

sub GetFiles {
    my ($negozio, $ip, $dataInUso) = @_;
    
    my $dbh;
    my $sth;
    
    #print "inizio: $negozio, $dataInUso, $ip\n";
    # connessione al database di sede
    $dbh = DBI->connect("dbi:ODBC:Driver={MySQL ODBC 5.3 Unicode Driver};Server=localhost;UID=root;PWD=mela");
    if (! $dbh) {
        print "Errore durante la connessione al database di default!\n";
        return 0;
    }
    
    my $maxSequenceNumber = -1;
    $sth = $dbh->prepare(qq{select ifnull(max(sequencenumber),0) max_sequence_number from mtx.idc where ddate = ? and store = ?;});
    if ($sth->execute($dataInUso,$negozio)) {
        $maxSequenceNumber = $sth->fetchrow_array();
    }
    $sth->finish();
    
    if (my $mtxDbh = DBI->connect("dbi:ODBC:Driver={SQL Server};Server=$ip;UID=mtxadmin;PWD=mtxadmin")) {
        $mtxDbh->{RaiseError} = 0;
        $mtxDbh->{PrintError} = 0;
        $mtxDbh->do("use mtx");
        
        # controllo se ci sono record nella IDC. Se non li trovo la giornata è chiusa e cerco nella IDC_EOD
        my $status = 0;
        my $tableInUse = 'IDC';
        if ($dataInUso ne $dataCorrente->ymd('-')) {
            $status = 1;
            $tableInUse = 'IDC_EOD';   
        }
        if ($status == 0) {
            my $mtxSth = $mtxDbh->prepare("select isnull(count(*), 0) from IDC where DDATE = ? ");
            if ($mtxSth->execute($dataInUso)) {
                my @count = $mtxSth->fetchall_arrayref();
                if (! $count[0][0][0]) {
                    $tableInUse = 'IDC_EOD';
                    
                    $mtxSth = $mtxDbh->prepare("select isnull(max(sequencenumber), 0) from IDC_EOD where DDATE = ? ");
                    if ($mtxSth->execute($dataInUso)) {
                        my @countEod = $mtxSth->fetchall_arrayref();
                        if ($count[0][0][0] == 0 && $countEod[0][0][0] != 0) {
                            $status = 1;
                            if ($countEod[0][0][0] == $maxSequenceNumber) {
                                $status = 2;
                            }
                        } 
                    }
                }
            }
        } else {
            my $mtxSth = $mtxDbh->prepare("select isnull(max(sequencenumber), 0) from IDC_EOD where DDATE = ? ");
            if ($mtxSth->execute($dataInUso)) {
                my @count = $mtxSth->fetchall_arrayref();
                if ($count[0][0][0] == $maxSequenceNumber) {
                    $status = 2;
                }
                
            }
        }
        $sth = $dbh->prepare(qq{update mtx.eod set status = ?, modified_at = now() where ddate = ? and store = ?;});
        $sth->execute($status, $dataInUso,$negozio);
        $sth->finish();
            
        my $sql ="	select top 10000
                            REG, STORE, substring(convert(VARCHAR, DDATE, 120),1,10) 'DDATE', TTIME, SEQUENCENUMBER,
                            TRANS, TRANSSTEP, RECORDTYPE, RECORDCODE, USERNO, MISC, DATA
                        from $tableInUse
                        where sequencenumber > ? and DDATE = ?
                        order by sequencenumber;";
            
        my $totale_corrente_importo = 0;
        my $totale_corrente_clienti = 0;
        my $mtxSth = $mtxDbh->prepare ($sql);
        if ($mtxSth->execute($maxSequenceNumber, $dataInUso)) {
            # recupero i dati in una sola chiamata invece che ciclare
            my $dc = $mtxSth->fetchall_arrayref();

            # faccio in modo che l'ultimo record caricato sia un "F" così ho la certezza che ogni scontrino
            # sia completo.
            my $lastValidIndex = -1;
            for(my $i = @{$dc} - 1;$i >= 0;$i--) {
                if ($dc->[$i][7] =~ /^F$/ && $lastValidIndex < 0) {
                    $lastValidIndex = $i;
                }
            }
            if ($lastValidIndex >= 0) {
                @{$dc} = splice(@{$dc}, 0, $lastValidIndex + 1);
            }
            
            # se il @dc contiene righe posso caricare i dati
            if (@{$dc}) {
                 $sql = "insert into `mtx`.`idc` 
                        (`reg`,`store`,`ddate`,`ttime`,`hour`,`sequencenumber`,`trans`,`transstep`,`recordtype`,
                        `recordcode1`,`recordcode2`,`recordcode3`,`userno`,`misc`,`data`,`saleid`,`amount`,`totalamount`,
                       `taxcode`,`totaltaxableamount`,`taxamount`,`barcode`,`quantita`,`totalpoints`,`paymentform`,`created_at`)
                    values
                        (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,now())
                    on duplicate key update 
                        `reg` = ?,`store`= ?,`ddate`= ?,`ttime`= ?,`hour`= ?,`sequencenumber`= ?,`trans`= ?,`transstep`= ?,`recordtype`= ?,
                        `recordcode1`= ?,`recordcode2`= ?,`recordcode3`= ?,`userno`= ?,`misc`= ?,`data`= ?,`saleid`= ?,`amount`= ?,`totalamount`= ?,
                        `taxcode`= ?,`totaltaxableamount`= ?,`taxamount`= ?, `barcode`= ?,`quantita`= ?,`totalpoints`= ?,`paymentform`= ?,`created_at` = now();";
                my $sth = $dbh->prepare(qq{$sql});
                
                for (my $i=0;$i<@{$dc};$i++) {
                    my $tipo = $dc->[$i][7];
                    my $codice1 = '';
                    my $codice2 = '';
                    my $codice3 = '';
                    if ($dc->[$i][8] =~ /^(.)(.)(.)$/) {
                        $codice1 = $1;
                        $codice2 = $2;
                        $codice3 = $3;
                    }
                    if (($tipo !~ /(i|v|V)/) || ($tipo =~ /V/ && $codice3 =~ /1/) || ($tipo =~ /i/ && $codice2 =~ /(e|f)/)) {
                        my $cassa = sprintf('%03d', $dc->[$i][0]);
                        my $data = $dc->[$i][2];
                        my $ora = $dc->[$i][3];
                        my $fasciaOraria = '';
                        if ($ora =~ /^(\d\d)\d{4}$/) {
                            $fasciaOraria = $1;
                        }
                        my $sequenzaDc = $dc->[$i][4];
                        my $transazione = $dc->[$i][5];
                        my $sequenzaTransazione = $dc->[$i][6];
                        my $utente =  $dc->[$i][9];
                        my $misc =  $dc->[$i][10];
                        my $dati =  $dc->[$i][11];
                        my $numeroVendita = 0;
                        my $codiceIva = 0;         
                        my $prezzoUnitario = 0;
                        my $totaleImposta = 0;
                        my $totaleVendita = 0;
                        my $totaleVenditaNettoSconti = 0;
                        my $barcode = '';
                        my $quantita = 0;
                        my $totalePunti = 0;
                        my $formaPagamento = '';
                        if ($tipo =~ /^S$/) {
                            if ($dc->[$i+2][11] =~ /^\:(\d{4})/) {
                                $numeroVendita = $1 * 1;
                            }
                            if ($dc->[$i+1][11] =~ /^\:(\d{11})/) {
                                $codiceIva = $1 * 1;
                            }
                            my $idDettaglioIva = -1;
                            my $j = $i + 3;
                            while ($j < @{$dc} && $dc->[$i][2] eq $dc->[$j][2] && $dc->[$i][5] eq $dc->[$j][5] && $idDettaglioIva < 0) {
                                if ($dc->[$i + 2][11] eq $dc->[$j][11] && $dc->[$j][7] =~ /v/) {
                                    $idDettaglioIva = $j;
                                }
                                $j++;
                            }
                            if ($idDettaglioIva >= 0) {
                                if ($dc->[$idDettaglioIva][7] =~ /v/  ) {
                                    if ($dc->[$idDettaglioIva - 1][11] =~ /^(\+|\-)\d{4}(\d{7})(\d{7})/  ) {
                                        $totaleImposta = ($1.$3) / 100;
                                        $totaleVenditaNettoSconti = ($1.$2) / 100;
                                    }
                                }
                            }
                            $barcode = $misc;
                            $barcode =~ s/\s//g;
                            if ($dati =~ /^.{5}\./) {
                                if ($dati =~ /^(.{9})(.{10})/) {
                                    $quantita = $1 * 1;
                                    if ($quantita != 0) {
                                       $prezzoUnitario = sprintf("%.2f", $2/$1/100);
                                    }
                                    $totaleVendita = $2 / 100;
                                }
                            } elsif ($dati =~ /^((?:\+|\-)\d{4}).{5}(\d{9})$/) {
                                    $quantita = $1 * 1;
                                    $prezzoUnitario = $2 / 100;
                                    $totaleVendita = $prezzoUnitario * $quantita;
                            }
                        }
    
                        if ($tipo =~ /^C$/) {
							if ($misc =~ /^.{3}(.{13}$)/) {
								$barcode = $1;
							}
                            $barcode =~ s/\s//g;
                            if ($dati =~ /^((?:\+|\-)\d{4}).{4}((?:\+|\-)\d{9})$/) {
                                    $quantita = $1 * 1;
                                    $totaleVendita = $2 / 100; #sconto totale
                                    if ($quantita != 0) {
										$prezzoUnitario = sprintf("%.2f", $totaleVendita / $1); #sconto unitario
									}   
                            }
						}
                        
                        if ($tipo =~ /^D$/) {
							if ($dati =~ /((?:\+|\-)\d{9})$/) {
                                    $totaleVendita = $1 / 100; #sconto totale 
                            }
						}
                        
                        if ($tipo =~ /V/) {
                            if ($dati =~ /((?:\+|\-)\d{9})$/) {
                               $prezzoUnitario = $1 / 100; #imponibile
                            }
                            
                            if ( $dc->[$i + 1][7] =~ /V/ && $dc->[$i + 1][8] =~ /0$/) {
                                if ($dc->[$i + 1][11] =~ /((?:\+|\-)\d{9})$/) {
                                    $totaleImposta = $1 / 100; #imposta totale per l'aliquota indicata 
                                }
                            }
                            $totaleVendita = $prezzoUnitario + $totaleImposta; #lordo x aliquota 
                            $codiceIva = $codice2;
                        }
                        
                        if ($tipo =~ /^G$/) {
							if ($misc =~ /^.{3}(.{13}$)/) {
								$barcode = $1;
							}
                            $barcode =~ s/\s//g;
                            if ($dati =~ /^\:00((?:\+|\-)\d{5})((?:\+|\-)\d{9})$/) {
                                    $quantita = 1;
                                    $totaleVendita = $2 / 100; #importo di riferimento
                                    $totalePunti = $1 * 1; 
                            }
						}
                        
                        if ($tipo =~ /^F$/) {
							if ($dati =~ /((?:\+|\-)\d{9})$/) {
                                    $totaleVendita = $1 / 100; #totale scontrino
                            }
						}
                        
                        if ($tipo =~ /^T$/) {
							if ($dati =~ /^\:(\d\d).{6}((?:\+|\-)\d{9})$/) {
                                    $quantita = 1;
                                    $formaPagamento = $1;
                                    $totaleVendita = $2 / 100; #importo pagamento 
                            }
						}
                        
                         if ($tipo =~ /^(k|w)$/) {
							$barcode = $misc; #
                            $barcode =~ s/\s//g;
						}
						
                        $sth->execute(
                            $cassa, $negozio, $data, $ora, $fasciaOraria, $sequenzaDc, $transazione, $sequenzaTransazione,
                            $tipo, $codice1, $codice2, $codice3, $utente, $misc, $dati,
                            $numeroVendita, $prezzoUnitario, $totaleVendita, $codiceIva, $totaleVenditaNettoSconti, $totaleImposta,  $barcode, $quantita,
                            $totalePunti, $formaPagamento,
                            $cassa, $negozio, $data, $ora, $fasciaOraria, $sequenzaDc, $transazione, $sequenzaTransazione,
                            $tipo, $codice1, $codice2, $codice3, $utente, $misc, $dati,
                            $numeroVendita, $prezzoUnitario, $totaleVendita, $codiceIva, $totaleVenditaNettoSconti, $totaleImposta, $barcode, $quantita,
                            $totalePunti, $formaPagamento);
                    }
                }
            }            
        }
        
        $sth->finish();
        $dbh->disconnect();
        $mtxSth->finish();
        $mtxDbh->disconnect();
    
    }
    
    #print "Fine: $negozio, $dataInUso, $ip\n";
    return 1;
}

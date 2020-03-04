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
#my $dataCorrente 	= DateTime->now(time_zone=>'CET');
#my $currentTime 	= DateTime->now(time_zone=>'local');
my $dataCorrente	= DateTime->new(year=>2020, month=>3, day=>1);

# handler/variabili globali
#------------------------------------------------------------------------------------------------------------
my @thr;
my %negoziDettagli = (
    '0101' => {'descrizione' => '0101 - PIAVE', 'ip' => '192.168.201.11'},
    '0102' => {'descrizione' => '0102 - MARCHETTI', 'ip' => '192.168.202.11'},
    '0103' => {'descrizione' => '0103 - COLLEBEATO', 'ip' => '192.168.203.11'},
    '0105' => {'descrizione' => '0105 - MOMPIANO', 'ip' => '192.168.205.11'},
    '0106' => {'descrizione' => '0106 - ARGENTINA', 'ip' => '192.168.206.11'},
    '0107' => {'descrizione' => '0107 - PIADENA', 'ip' => '192.168.207.11'},
    '0108' => {'descrizione' => '0108 - DESENZANO', 'ip' => '192.168.208.11'},
    '0109' => {'descrizione' => '0109 - SALO', 'ip' => '192.168.209.11'},
    '0110' => {'descrizione' => '0110 - SAREZZO', 'ip' => '192.168.210.11'},
    '0111' => {'descrizione' => '0111 - VALCENTER', 'ip' => '192.168.223.11'},
    '0113' => {'descrizione' => '0113 - LENO', 'ip' => '192.168.213.11'},
    '0114' => {'descrizione' => '0114 - ROE VOLCIANO', 'ip' => '192.168.17.11'},
    '0115' => {'descrizione' => '0115 - PALAZZOLO', 'ip' => '192.168.4.11'},
    '0119' => {'descrizione' => '0119 - PRALBOINO', 'ip' => '192.168.219.11'},
    '0121' => {'descrizione' => '0121 - BREGNANO', 'ip' => '192.168.121.11'},
    '0122' => {'descrizione' => '0122 - COGLIATE', 'ip' => '192.168.122.11'},
    '0123' => {'descrizione' => '0123 - SOVERE', 'ip' => '192.168.123.11'},
    '0124' => {'descrizione' => '0124 - MOZZANICA', 'ip' => '192.168.224.11'},
    '0125' => {'descrizione' => '0125 - CISANO', 'ip' => '192.168.225.11'},
    '0126' => {'descrizione' => '0126 - CORNATE', 'ip' => '192.168.26.11'},
    '0127' => {'descrizione' => '0127 - MANERBIO', 'ip' => '192.168.227.11'},
    '0128' => {'descrizione' => '0128 - NEW-SALO', 'ip' => '192.168.228.11'},
    '0129' => {'descrizione' => '0129 - CASTEL GOFFREDO', 'ip' => '192.168.18.11'},
    '0131' => {'descrizione' => '0131 - CHIARI', 'ip' => '192.168.3.11'},
    '0132' => {'descrizione' => '0132 - PISOGNE', 'ip' => '192.168.13.11'},
    '0133' => {'descrizione' => '0133 - ALBERTANO', 'ip' => '192.168.233.11'},
    '0134' => {'descrizione' => '0134 - S.EUFEMIA', 'ip' => '11.0.34.11'},
    '0136' => {'descrizione' => '0136 - PAVIA', 'ip' => '192.168.236.11'},
    '0138' => {'descrizione' => '0138 - CANNETO', 'ip' => '192.168.238.11'},
    '0139' => {'descrizione' => '0139 - GORLE SUPERM.', 'ip' => '192.168.239.11'},
    '0140' => {'descrizione' => '0140 - ROCCAFRANCA', 'ip' => '192.168.240.11'},
    '0141' => {'descrizione' => '0141 - ORZINUOVI', 'ip' => '192.168.7.11'},
    '0142' => {'descrizione' => '0142 - SARNICO', 'ip' => '192.168.242.11'},
    '0143' => {'descrizione' => '0143 - VEROLANUOVA', 'ip' => '192.168.243.11'},
    '0144' => {'descrizione' => '0144 - QUINZANO', 'ip' => '192.168.244.11'},
    '0145' => {'descrizione' => '0145 - GHEDI', 'ip' => '192.168.245.11'},
    '0146' => {'descrizione' => '0146 - OSPITALETTO', 'ip' => '192.168.2.11'},
    '0147' => {'descrizione' => '0147 - CARPENEDOLO', 'ip' => '192.168.6.11'},
    '0148' => {'descrizione' => '0148 - ASOLA', 'ip' => '192.168.5.11'},
    '0149' => {'descrizione' => '0149 - VALEGGIO', 'ip' => '192.168.15.11'},
    '0153' => {'descrizione' => '0153 - SAN COLOMBANO', 'ip' => '192.168.153.11'},
    '0155' => {'descrizione' => '0155 - TREVIGLIO', 'ip' => '192.168.155.11'},
    '0156' => {'descrizione' => '0156 - STEZZANO', 'ip' => '192.168.156.11'},
    '0170' => {'descrizione' => '0170 - GUSSAGO', 'ip' => '192.168.9.11'},
    '0171' => {'descrizione' => '0171 - FRECCIAROSSA', 'ip' => '192.168.141.11'},
    '0172' => {'descrizione' => '0172 - ISORELLA', 'ip' => '192.168.172.11'},
    '0173' => {'descrizione' => '0173 - MONTIRONE', 'ip' => '192.168.173.11'},
    '0176' => {'descrizione' => '0176 - CASALMAGGIORE', 'ip' => '192.168.176.11'},
    '0177' => {'descrizione' => '0177 - SONICO', 'ip' => '192.168.16.11'},
    '0178' => {'descrizione' => '0178 - BAGNOLO MELLA', 'ip' => '192.168.12.11'},
    '0179' => {'descrizione' => '0179 - VILLANUOVA SUL CLISI', 'ip' => '192.168.14.11'},
    '0180' => {'descrizione' => '0180 - MONIGA', 'ip' => '192.168.11.11'},
    '0181' => {'descrizione' => '0181 - CREMONA', 'ip' => '192.168.10.11'},
    '0184' => {'descrizione' => '0184 - SIRMIONE', 'ip' => '192.168.184.11'},
    '0185' => {'descrizione' => '0185 - MUGGIO', 'ip' => '192.168.185.11'},
    '0186' => {'descrizione' => '0186 - CAIRATE', 'ip' => '192.168.186.11'},
    '0188' => {'descrizione' => '0188 - CALCINATO 2', 'ip' => '192.168.188.11'},
    '0190' => {'descrizione' => '0190 - GHEDI 2', 'ip' => '192.168.190.11'},
    '0461' => {'descrizione' => '0461 - ISEO', 'ip' => '192.168.161.11'},
    '0462' => {'descrizione' => '0462 - CASTELLEONE', 'ip' => '192.168.162.11'},
    '0463' => {'descrizione' => '0463 - BEDIZZOLE', 'ip' => '192.168.163.11'},
    '0464' => {'descrizione' => '0464 - GRUMELLO DEL MONTE', 'ip' => '192.168.164.11'},
    '0465' => {'descrizione' => '0465 - SAN PANCRAZIO', 'ip' => '192.168.165.11'},
    '0466' => {'descrizione' => '0466 - TRESCORE BALNEARIO', 'ip' => '192.168.166.11'},
    '0467' => {'descrizione' => '0467 - CAPRIOLO', 'ip' => '192.168.167.11'},
    '0468' => {'descrizione' => '0468 - SAN MARTINO SICCOMARIO', 'ip' => '192.168.168.11'},
    '3151' => {'descrizione' => '3151 - FLERO', 'ip' => '172.30.10.2'},
    '3152' => {'descrizione' => '3152 - NAVE', 'ip' => '172.30.18.2'},
    '3650' => {'descrizione' => '3650 - VIA CHIESA', 'ip' => '172.30.2.2'},
    '3652' => {'descrizione' => '3652 - VOBARNO', 'ip' => '172.30.30.2'},
    '3654' => {'descrizione' => '3654 - LUMEZZANE', 'ip' => '192.168.154.11'},
    '3657' => {'descrizione' => '3657 - TRAVAGLIATO 1', 'ip' => '172.30.26.2'},
    '3658' => {'descrizione' => '3658 - GAVARDO', 'ip' => '172.30.13.2'},
    '3659' => {'descrizione' => '3659 - GARDONE V.T.', 'ip' => '172.30.12.2'},
    '3661' => {'descrizione' => '3661 - PADERNO F/C', 'ip' => '192.168.170.11'},
    '3665' => {'descrizione' => '3665 - CALVISANO', 'ip' => '172.30.4.2'},
    '3666' => {'descrizione' => '3666 - CASTELMELLA 1', 'ip' => '172.30.6.2'},
    '3668' => {'descrizione' => '3668 - TRAVAGLIATO 2', 'ip' => '172.30.27.2'},
    '3670' => {'descrizione' => '3670 - BORGO S.GIACOMO', 'ip' => '172.30.1.2'},
    '3671' => {'descrizione' => '3671 - CASTENEDOLO', 'ip' => '172.30.8.2'},
    '3673' => {'descrizione' => '3673 - VILLA CARCINA', 'ip' => '172.30.29.2'},
    '3674' => {'descrizione' => '3674 - SORESINA', 'ip' => '172.30.25.2'},
    '3675' => {'descrizione' => '3675 - GUIDIZZOLO', 'ip' => '172.30.14.2'},
    '3682' => {'descrizione' => '3682 - BOARIO', 'ip' => '172.30.0.2'},
    '3683' => {'descrizione' => '3683 - COSTA VOLPINO', 'ip' => '172.30.37.2'},
    '3687' => {'descrizione' => '3687 - RAFFA DI PUEGNAGO', 'ip' => '172.30.23.2'},
    '3689' => {'descrizione' => '3689 - CASTELMELLA 2', 'ip' => '172.30.7.2'},
    '3692' => {'descrizione' => '3692 - NAVE BARCELLA', 'ip' => '172.30.31.2'},
    '3693' => {'descrizione' => '3693 - PIZZIGHETTONE', 'ip' => '172.30.33.2'},
    '3694' => {'descrizione' => '3694 - AZZANO MELLA', 'ip' => '192.168.169.11'}
);

my @negozi = keys %negoziDettagli;

my $semaforo = 0;
my $dbh = DBI->connect("dbi:ODBC:Driver={MySQL ODBC 5.3 Unicode Driver};Server=localhost;UID=root;PWD=mela");
my $sth = $dbh->prepare("select ifnull(count(*),0) from `log`.`semaforo` where tipo =  10");
if ($sth->execute()) {
    $semaforo = $sth->fetchrow_array();
}
$sth->finish();

if (! $semaforo) {
    $sth = $dbh->prepare("insert into `log`.`semaforo` (`tipo`,`stato`) values (10, 100)");
    $sth->execute();
    
    #&GetFiles('3652', $negoziDettagli{'3652'}{'ip'}); #DEBUG ONLY
    
    for (my $i=0; $i<@negozi; $i++) {
        push @thr, threads->create('GetFiles', $negozi[$i], $negoziDettagli{$negozi[$i]}{'ip'}); 
    }
    
    #con l'istruzione join faccio in modo che l'esecuzione aspetti fino a che l'ultimo thread sia terminato
    for (my $j=0; $j<@thr; $j++) {
        $thr[$j]->join();
    }
    
    $sth = $dbh->prepare("delete from `log`.`semaforo` where tipo = 10;");
    $sth->execute();
    $sth->finish();
}

$dbh->disconnect();

exit;

sub GetFiles {
    my ($negozio, $ip) = @_;
    
    my $dbh;
    my $sth;
    
    # connessione al database di sede
    $dbh = DBI->connect("dbi:ODBC:Driver={MySQL ODBC 5.3 Unicode Driver};Server=localhost;UID=root;PWD=mela");
    if (! $dbh) {
        print "Errore durante la connessione al database di default!\n";
        return 0;
    }
    
    my $maxSequenceNumber = -1;
    $sth = $dbh->prepare(qq{select ifnull(max(sequencenumber),0) max_sequence_number from mtx.idc where ddate = ? and store = ?;});
    if ($sth->execute($dataCorrente->ymd('-'),$negozio)) {
        $maxSequenceNumber = $sth->fetchrow_array();
    }
    
    $sth->finish();
    
    if (my $mtxDbh = DBI->connect("dbi:ODBC:Driver={SQL Server};Server=$ip;UID=mtxadmin;PWD=mtxadmin")) {
        $mtxDbh->{RaiseError} = 0;
        $mtxDbh->{PrintError} = 0;
        $mtxDbh->do("use mtx");
        
        my $sql ="	select top 10000
                            REG, STORE, substring(convert(VARCHAR, DDATE, 120),1,10) 'DDATE', TTIME, SEQUENCENUMBER,
                            TRANS, TRANSSTEP, RECORDTYPE, RECORDCODE, USERNO, MISC, DATA
                        from IDC_EOD
                        where sequencenumber > ? and DDATE = ?
                        order by sequencenumber;";
            
        my $totale_corrente_importo = 0;
        my $totale_corrente_clienti = 0;
        my $mtxSth = $mtxDbh->prepare ($sql);
        if ($mtxSth->execute($maxSequenceNumber, $dataCorrente->ymd('-'))) {
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
                        (`reg`,`store`,`ddate`,`ttime`,`hour`,`sequencenumber`,`trans`,`transstep`,
                        `recordtype`,`recordcode1`,`recordcode2`,`recordcode3`,`userno`,`misc`,`data`,
                        `saleid`,`amount`,`totalamount`,`taxcode`,`totaltaxableamount`,`taxamount`,`barcode`,`quantita`,`totalpoints`,`paymentform`)
                    values
                        (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    on duplicate key update 
                        `reg` = ?,`store`= ?,`ddate`= ?,`ttime`= ?,`hour`= ?,`sequencenumber`= ?,`trans`= ?,`transstep`= ?,`recordtype`= ?,
                        `recordcode1`= ?,`recordcode2`= ?,`recordcode3`= ?,`userno`= ?,`misc`= ?,`data`= ?,`saleid`= ?,
                        `amount`= ?,`totalamount`= ?,`taxcode`= ?,`totaltaxableamount`= ?,`taxamount`= ?, `barcode`= ?,`quantita`= ?,`totalpoints`= ?,`paymentform`= ?;";
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
                               $totaleVendita = $1 / 100; #lordo x aliquota 
                            }
                            
                            if ( $dc->[$i + 1][7] =~ /V/ && $dc->[$i + 1][8] =~ /0$/) {
                                if ($dc->[$i + 1][11] =~ /((?:\+|\-)\d{9})$/) {
                                    $totaleImposta = $1 / 100; #imposta x aliquota 
                                }
                            }
                            $prezzoUnitario = $totaleVendita - $totaleImposta; #imponibile
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
                        
                         if ($tipo =~ /^k$/) {
							if ($misc =~ /^.{3}(.{13}$)/) {
								$barcode = $1;
							}
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
    return 1;
}

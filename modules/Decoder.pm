package Decoder;

use strict;
use warnings;

use DBI;
use DateTime;

# parametri db
# ---------------------------------------------
my $ip   = '127.0.0.1'; #'10.11.14.128';
my $user = 'root';
my $pw   = 'mela';

INIT {
    #$dbh = DBI->connect("dbi:ODBC:Driver={MySQL ODBC 5.3 Unicode Driver};Server=$ip;UID=$user;PWD=$pw");
    my $dbh = DBI->connect("DBI:mysql:mysql:$ip", $user, $pw);
    if (! $dbh) {
        die "Errore durante la connessione al database $ip!\n";
    }
    
    my $sth = $dbh->prepare('create database if not exists mtx');
    $sth->execute() or die('Impossibile creare il datbase mtx');
    
    my $sql = "CREATE TABLE IF NOT EXISTS `mtx`.`idc` (
                `reg` varchar(3) NOT NULL DEFAULT '',
                `store` varchar(4) NOT NULL DEFAULT '',
                `ddate` date NOT NULL,
                `ttime` varchar(6) NOT NULL DEFAULT '000000',
                `hour` varchar(2) NOT NULL,
                `sequencenumber` int(11) unsigned NOT NULL,
                `trans` smallint(5) unsigned NOT NULL,
                `transstep` smallint(5) unsigned NOT NULL,
                `recordtype` varchar(1) NOT NULL DEFAULT '',
                `recordcode1` varchar(1) NOT NULL DEFAULT '',
                `recordcode2` varchar(1) NOT NULL,
                `recordcode3` varchar(1) NOT NULL,
                `userno` smallint(5) unsigned NOT NULL,
                `misc` varchar(16) NOT NULL DEFAULT '',
                `data` varchar(19) NOT NULL DEFAULT '',
                `saleid` smallint(5) unsigned NOT NULL DEFAULT 0,
                `taxcode` tinyint(3) unsigned NOT NULL DEFAULT 0,
                `amount` decimal(11,2) NOT NULL DEFAULT 0.00,
                `totalamount` decimal(11,2) NOT NULL DEFAULT 0.00,
                `totaltaxableamount` decimal(11,2) NOT NULL DEFAULT 0.00,
                `taxamount` decimal(11,2) NOT NULL DEFAULT 0.00,
                `barcode` varchar(13) NOT NULL DEFAULT '',
                `quantita` decimal(7,3) NOT NULL DEFAULT 0.000,
                `totalpoints` smallint(6) NOT NULL,
                `paymentform` varchar(2) NOT NULL DEFAULT '',
                `actioncode` varchar(2) DEFAULT NULL,
                `created_at` timestamp NULL DEFAULT NULL,
                PRIMARY KEY (`store`,`ddate`,`sequencenumber`),
                KEY `recordtype` (`recordtype`),
                KEY `store` (`store`,`ddate`),
                KEY `barcode` (`barcode`),
                KEY `created_at` (`created_at`),
                KEY `store_2` (`store`,`ddate`,`reg`,`trans`),
                KEY `incassi` (`ddate`,`store`,`recordtype`,`recordcode1`),
                KEY `actioncode` (`actioncode`),
                KEY `recordtype_2` (`recordtype`,`actioncode`),
                KEY `recordtype_3` (`recordtype`,`paymentform`)
              ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;";

    $sth = $dbh->prepare(qq{$sql});
    $sth->execute() or die('Impossibile creare il la tabella IDC');
    $sth->finish();
    
    $sql = "CREATE TABLE IF NOT EXISTS `mtx`.`EOD` (
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
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;";

    $sth = $dbh->prepare(qq{$sql});
    $sth->execute() or die('Impossibile creare il la tabella EOD');
    $sth->finish();
    
    $sql = "CREATE TABLE IF NOT EXISTS `mtx`.`biz` (
                `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
                `store` varchar(4) NOT NULL DEFAULT '',
                `ddate` date NOT NULL,
                `reg` varchar(3) NOT NULL DEFAULT '',
                `trans` varchar(4) NOT NULL DEFAULT '',
                `transstep` smallint(6) NOT NULL,
                `ttime` varchar(6) NOT NULL DEFAULT '',
                `idBiz` int(11) NOT NULL,
                `ddateBiz` date NOT NULL,
                `plu` int(11) NOT NULL,
                `weight` int(11) NOT NULL,
                `prize` int(11) NOT NULL,
                `prizePerKg` int(11) NOT NULL,
                PRIMARY KEY (`store`,`ddate`,`reg`,`trans`,`transstep`),
                KEY `id` (`id`)
            ) ENGINE=InnoDB AUTO_INCREMENT=11 DEFAULT CHARSET=utf8mb4;";

    $sth = $dbh->prepare(qq{$sql});
    $sth->execute() or die('Impossibile creare il la tabella BIZ');
    $sth->finish();
    
    $dbh->disconnect();
}

sub getLastUsedSequenceNumber {
    my($store, $ddate, $reg, $trans) = @_;
    
    #$dbh = DBI->connect("dbi:ODBC:Driver={MySQL ODBC 5.3 Unicode Driver};Server=$ip;UID=$user;PWD=$pw");
    my $dbh = DBI->connect("DBI:mysql:mysql:$ip", $user, $pw);
    if (! $dbh) {
        die "Errore durante la connessione al database $ip!\n";
    }
    
    my $sql = "  select min(sequencenumber) firstSequenceNumber
                from mtx.idc where store = $store and ddate = $ddate and reg = $reg and trans = $trans";
    my $sth = $dbh->prepare(qq{$sql});
    my $firstSequenceNumber = 0;
    $sth->bind_col(1,\$firstSequenceNumber);  
    $sth->execute();
    $sth->fetch;
    
    $sth->finish();
    $dbh->disconnect();
    
    return $firstSequenceNumber;
}

sub loadDC {
    my($dc, $negozio) = @_;
    
    #$dbh = DBI->connect("dbi:ODBC:Driver={MySQL ODBC 5.3 Unicode Driver};Server=$ip;UID=$user;PWD=$pw");
    my $dbh = DBI->connect("DBI:mysql:mysql:$ip", $user, $pw);
    if (! $dbh) {
        die "Errore durante la connessione al database $ip!\n";
    }
    
    my $sql = "insert into `mtx`.`idc` 
                    (`reg`,`store`,`ddate`,`ttime`,`hour`,`sequencenumber`,`trans`,`transstep`,`recordtype`,
                    `recordcode1`,`recordcode2`,`recordcode3`,`userno`,`misc`,`data`,`saleid`,`amount`,`totalamount`,
                   `taxcode`,`totaltaxableamount`,`taxamount`,`barcode`,`quantita`,`totalpoints`,`paymentform`,`actioncode`,`created_at`)
                values
                    (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,now())
                on duplicate key update 
                    `reg` = ?,`store`= ?,`ddate`= ?,`ttime`= ?,`hour`= ?,`sequencenumber`= ?,`trans`= ?,`transstep`= ?,`recordtype`= ?,
                    `recordcode1`= ?,`recordcode2`= ?,`recordcode3`= ?,`userno`= ?,`misc`= ?,`data`= ?,`saleid`= ?,`amount`= ?,`totalamount`= ?,
                    `taxcode`= ?,`totaltaxableamount`= ?,`taxamount`= ?, `barcode`= ?,`quantita`= ?,`totalpoints`= ?,`paymentform`= ?,`actioncode`= ?, `created_at` = now();";
                
    my $sth = $dbh->prepare(qq{$sql});
                
    my $sqlBiz = "  insert ignore into `mtx`.`biz`
                        (`store`,`ddate`,`reg`,`trans`,`transstep`,`ttime`,`idBiz`,`ddateBiz`,`plu`,`weight`,`prize`,`prizePerKg`)
                    values (?,?,?,?,?,?,?,?,?,?,?,?)";
    my $sthBiz = $dbh->prepare(qq{$sqlBiz});
                
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
            my $actionCode = '';
            if ($tipo =~ /^S$/) {
                if ($dc->[$i+2][11] =~ /^\:(\d{4})/) {
                    $numeroVendita = $1 * 1;
                }
                if ($dc->[$i+1][11] =~ /^\:\d{4}(\d{7})/) {
                    $codiceIva = $1 * 1;
                }
                my $idDettaglioIva = -1;
                my $j = $i + 3;
                while ($j < @{$dc} && $dc->[$i][2] eq $dc->[$j][2] && $dc->[$i][5] eq $dc->[$j][5] && $idDettaglioIva < 0) {
                    if (substr($dc->[$i + 2][11],0,5) eq substr($dc->[$j][11],0,5) && $dc->[$j][7] =~ /v/) {
                        $idDettaglioIva = $j;
                    }
                    $j++;
                }
                if ($idDettaglioIva >= 0) {
                    if ($dc->[$idDettaglioIva][7] =~ /v/  ) {
                        if ($dc->[$idDettaglioIva - 1][11] =~ /^(\+|\-)\d{4}(\d{7})(\d{7})/  ) {
                            $totaleImposta = ($1.$3) / 100;
                            $totaleVenditaNettoSconti = ($1.$2) / 100;
                        } elsif ($dc->[$idDettaglioIva - 1][11] =~ /^\+\d{4}\-(\d{6})\-(\d{6})/  ) {
                            $totaleImposta = $2 / 100;
                            $totaleVenditaNettoSconti = $1 / 100; 
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
                
                if ($dc->[$i+3][7] =~ /^i$/ && $dc->[$i + 3][8] =~ /^..3$/) {
                    if ($dc->[$i+3][11] =~ /^\:(\d{2})(\d{2})(\d{2})\:(.{6})/) {
                        my $dataBiz = '20' . $1 . '-' . $2 . '-' . $3;
                        my $idBiz = $4 * 1;
                        if ($barcode =~ /^21(\d{4})\d{7}$/) {
                            $sthBiz->execute($negozio, $data, $cassa, $transazione, $sequenzaTransazione, $ora, $idBiz, $dataBiz, $1,
                                             sprintf("%.0f", $quantita * 1000),sprintf("%.0f", $totaleVendita * 100), sprintf("%.0f", $prezzoUnitario * 100));
                        }  
                    }
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
                
                if ($dati =~ /^:(\d\d)/) {
                        $actionCode = $1; #action code
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
                $totalePunti, $formaPagamento, $actionCode,
                $cassa, $negozio, $data, $ora, $fasciaOraria, $sequenzaDc, $transazione, $sequenzaTransazione,
                $tipo, $codice1, $codice2, $codice3, $utente, $misc, $dati,
                $numeroVendita, $prezzoUnitario, $totaleVendita, $codiceIva, $totaleVenditaNettoSconti, $totaleImposta, $barcode, $quantita,
                $totalePunti, $formaPagamento, $actionCode);
        }
    }
    $sth->finish();
    $dbh->disconnect();
}

1;
package Decoder;

use strict;
use warnings;

use DBI;
use DateTime;


# parametri di connessione al database quadrature
#------------------------------------------------------------------------------------------------------------
my $ip      = '10.11.14.128';
my $user    = 'root';
my $pw      = 'mela';


sub loadDC {
    my($dc) = @_;
    
    my $dbh = DBI->connect("dbi:ODBC:Driver={MySQL ODBC 5.3 Unicode Driver};Server=localhost;UID=root;PWD=mela");
    #my $dbh = DBI->connect("DBI:mysql:mtx:$ip", $user, $pw);
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
        
        for (my $i=0;$i<@{$dc};$i++) {
            my $negozio = $dc->[$i][1];
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
                
                if ($tipo =~ /^m$/) {
                    $misc = '00:' . $misc;
                }
                
                if ($tipo =~ /^z$/) {
                    $misc = substr(sprintf("%04d", $utente), 2) . ':' .$misc;
                    if ($misc =~ /^(.*)(...)$/) {
                        $misc = $1;
                        my $resto = $2;
                        if ($dati =~ /^(.*)...$/) {
                            $dati = $resto . $1;
                        }
                    }
                }
                
                if ($tipo =~ /^i$/) {
                    $dati =~ s/^\s+//ig;
                }
                
                $misc =~ s/^\s+//ig;
                
                #print "$cassa/$transazione/$codiceIva\n";
                
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
    }


$dbh->disconnect();


}

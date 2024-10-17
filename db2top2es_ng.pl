#!/usr/bin/perl -w
# -- errpt2elastic.pl
# -- runs as daemon, and sends new messages to elastic search cluster
# -- Author Jiri.Pavel@e-sys.cz
#


use strict;
use warnings;

use LWP;
use HTTP::Request;
use JSON;
use Data::Dumper;
use Digest::MD5;
use Time::Local;

$SIG{INT} = \&signal_handler;
$SIG{TERM} = \&signal_handler;
#
my( $config );
#my( %hmsg );
my( @aamsg );
my( $prerec );
my( $line );
my( $last_time ) = "" ;
my( $date ) = $ARGV[0];
my( $dbName ) = $ARGV[1];
my( $lineCount, $maxLines ) = ( 0, 1000 );


  $config->{'elasticDest'} = 'https://db2:db2db2db2@localhost:9200';
  $config->{'elasticDest'} = 'http://localhost:9200';
  #  $config->{'elasticDest'} = 'http://192.168.122.13:9200';
  $config->{'indexNameHB'} = 'db2top';
  $config->{'debug'} = 0;


  #$hmsg{'handle_id'} = 10;
  #$hmsg{'user'} = "d460889";
  #  $hmsg{'cpu_perc'} = 10.23;

  open( IN, "< &0"  ) ;

 $line=<IN> ; #-- skip header

while( $line=<IN> ) {
  $lineCount += 1;
  # -- if there is new time then reset values in appl., where values not updated
  if( $last_time ne ( split( /;/, $line) )[0] ) {
    &clean_prerec( $last_time );
  }

  push( @aamsg, &line2hash( $line, $date, $dbName ) );
#  push( @aamsg, \%hmsg );
  if( $lineCount > $maxLines ) {
    &send2elastic( $config->{'elasticDest'}, $config->{'indexNameHB'}, $config->{'ingestPipeHB'}, \@aamsg ) ;
    $lineCount = 0;
    @aamsg = ();
  }
}

#  push( @aamsg, \%hmsg );

  &send2elastic( $config->{'elasticDest'}, $config->{'indexNameHB'}, $config->{'ingestPipeHB'}, \@aamsg ) ;

  exit();


  
  
  
sub clean_prerec {
  my( $last_time ) = shift( @_ );
  
  foreach my $ap_handler  ( keys( %{$prerec} ) ) {
    if( defined ( $prerec->{$ap_handler}->{'time'} ) && $prerec->{$ap_handler}->{'time'} ne $last_time ){
      $prerec->{$ap_handler}->{'values'} = [];
    }
  }
}




sub line2hash {
  my( $line ) = shift( @_ );
  my( $date ) = shift( @_ );
  my( $dbName ) = shift( @_ );
  my( $variable ) ;
  my( $value ) ;
  my( $hout );
  my( $i );
  my( @ti );
  my( @head ) = (
    '@timestamp',
    'Application_Handle',
    'Cpu_perc',
    'IO_perc',
    'Mem_perc',
    'Application_Status',
    'Application_Name',
    'Delta_RowsRead',
    'Delta_RowsWritten',
    'Delta_IOReads',
    'Delta_IOWrites',
    'Delta_TQr',
    'Sess_Memory',
    'Assoc_Agents',
    'Paral_Degree',
    'Lockwait_',
    'Locks_Held',
    'Sorts_',
    'Log_Used',
    'Delta_RowsSelect',
    'Fetch_Count',
    'Dynamic_SQL',
    'Static_SQL',
    'num_of_XQueries',
    'Os_User',
    'DB_User',
    'Client_NetName',
    'Client_Platform',
    'Status_ChTime',
    'Time_InStatus',
    'IoType',
    'Sorts_Overflows',
    'Hash Join_Overflows',
    'Client_Pid',
    'Node_Number',
    'Last_Operation',
    'TimeTo_Connect',
    'Session_Cpu',
    'Statement_Cpu',
    'Max Cost_Estimate',
    'Wkd_Id',
    'Recent_Cpu',
      );

  my( $ap_handler )  = (split( /;/, $line ) )[1];
  my( $v_diff ) = 0;
  chomp( $date );
  my( $yy, $mm, $dd ) = split( /-/, $date );
  $mm -= 1;

  chomp( $line );
  $i = 0;
  foreach $value ( split( /;/, $line ) ) {

    if( $i < 1 ) { #-- prevod casu na timestamp
      ( @ti ) = split( /:/, $value );
      $value = timelocal( reverse( @ti ) , $dd, $mm, $yy ) * 1000;
    }

    $value =~ s/%//g;

    if( $i == 29) { #-- TimeInStatus - prevod hodnoty na jednoduche cislo [sec.]
    }

    if( $i == 15 || $i == 21 || $i == 22 || $i == 23 || $i == 31 || $i == 32 || $i == 37 ) {

      if( defined( ($prerec->{$ap_handler}->{'values'})->[$i] ) &&  ( ($prerec->{$ap_handler}->{'values'})->[$i] < $value  ) ) {

        $v_diff = $value - ($prerec->{$ap_handler}->{'values'})->[$i]; 

      }
      ($prerec->{$ap_handler}->{'values'})->[$i] =  $value ; 
      $value = $v_diff;
    } elsif( $i == 29 ) {
      $value = 0;
    }



    $hout->{ $head[$i] } = $value;
    $i++;
  }

  $hout->{'dbName'} = $dbName;

  return( $hout );

}

sub send2elastic {

        my( $dest  ) = shift( @_ );

        my( $index  ) = shift @_;

        my( $ingestPipe  ) = shift @_;

        my( $amsg ) = shift @_;
        my( $rmsg )  ;

        my( $data );

        my( $json );

        my( $key, $key1 );

        my( $browser );

        my( $response );

        my( %resp );

        my( $i );

        my( $is_all_send_ok ) = 1 ; #-- status, zda doslo k nejake chybe pri zpracovani udalosti na strane elasticu

        my( $sendState );





#--  foreach $key ( keys( %{$hash}) ) {
  foreach $rmsg ( @{$amsg} ) {

    $data .= "{\"index\":{}}\n";

    $data .= encode_json( $rmsg ) . "\n";

  }





  $browser = LWP::UserAgent->new();



  $response = $browser->post(

#       "http://172.16.11.120:9200/tst_errpt/_bulk/?pipeline=add_err_group"     #-- toto se bude menit

#       "${dest}/${index}/_bulk/"       #-- toto se bude menit

        "${dest}/${index}/_bulk/"       #-- toto se bude menit

        , 'Content-Type'=>"application/json"    #-- toto vzdy

        , 'Content'=>$data );                   #-- toto se bude menit



  print $data if $config->{'debug'};



  print "\n" if $config->{'debug'};

  print $response->content . "\n\n" if $config->{'debug'};


  if ($response->is_success) {

    print $response->content . "\n\n" if $config->{'debug'};

    %resp = %{ decode_json( $response->content )};

    foreach $key1 ( keys( %resp )){
      print $key1 . "\n" if $config->{'debug'};
    }

    foreach $i ( 0 .. $#{$resp{'items'}}  ) {

      if( $resp{'items'}[$i]{'index'}{'status'} != 201 ) {

        $is_all_send_ok  = 0;   #== doslo k nejake chybe pri zpracovani udalosti elasticem

      }



      if( $config->{'debug'} ) {

        print $resp{'items'}[$i]{'index'}{'result'}  if defined( $resp{'items'}[$i]{'index'}{'result'} ) ;

        print Dumper($resp{'items'}[$i]{'index'}{'error'})  if defined( $resp{'items'}[$i]{'index'}{'error'} ) ;

        print " sts: " . $resp{'items'}[$i]{'index'}{'status'} . "\n"  if $config->{'debug'};

      }

    }



#    print Dumper(%resp) if $config->{'debug'};

  }

  else {

    return 0;

    $sendState = Dumper(%resp);

  }



  if( ! $is_all_send_ok ) {

    $sendState = Dumper(%resp);

  }



  return $is_all_send_ok;

}



__END__;


Time
Application_Handle
Cpu_perc
IO_perc
Mem_perc
Application_Status
Application_Name
Delta_RowsRead/s
Delta_RowsWritten/s
Delta_IOReads/s
Delta_IOWrites/s
Delta_TQr+w/s
Sess_Memory
Assoc_Agents
Paral_Degree
Lockwait_
Locks_Held
Sorts_
Log_Used
Delta_RowsSelect/s
Fetch_Count(Stmt)
Dynamic_SQL
Static_SQL
num_of_XQueries
Os_User
DB_User
Client_NetName
Client_Platform
Status_ChTime
Time_InStatus
IoType
Sorts_Overflows
Hash Join_Overflows
Client_Pid
Node_Number
Last_Operation
TimeTo_Connect
Session_Cpu
Statement_Cpu
Max Cost_Estimate
Wkd_Id
Recent_Cpu
